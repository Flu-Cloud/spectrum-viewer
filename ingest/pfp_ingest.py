"""
pfp_ingest.py: ingest PFP (periodic-frame-power) numbers into a compact store.

PFP = power across a 10 ms frame (560 positions, ~17.86 us each) per 10 MHz
channel, one trace per sweep (the SEA schedule sweeps every ~90 s, dwelling 4 s
on each of the 18 channels). We store one int8-quantized BLOB per
(channel, capture): (sensor, freq, t, frame). Re-render any channel/time window
crisply, like the PSD layer.

Source CSVs are found by walking --root (default $SEA_DATA_ROOT, else
./SEA-DATA) and matching PFP_<YYYY-MM-DD>_<sensor>_<stat>.csv anywhere
underneath, so the folder layout does not matter.

The server reads this file directly. compact_db.py repacks it into a smaller
chunked form and swaps that in; that step is now optional and only affects
size and speed, not whether the viewer works. Re-running against a database
that is already compacted appends in the chunked shape rather than the row
shape, so next month's data lands where the server will read it.

    py pfp_ingest.py --list                          # what is on disk
    py pfp_ingest.py CBBT-Directional                # default stat max_peak
    py pfp_ingest.py CBBT-Directional --limit 30     # quick prototype
    py pfp_ingest.py CBBT-Directional --stat mean_rms
"""

import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _require  # noqa: F401  -- deps message instead of a traceback

import duckdb
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cbrs_files                                    # noqa: E402
import chunk_io                                      # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)   # repo root (scripts live in ingest/)
# Where the CBRS source CSVs live. Set SEA_DATA_ROOT to your copy, or pass
# --root; defaults to ./SEA-DATA in the repo so a fresh clone never points at
# someone else's disk.
DATA_ROOT = os.environ.get("SEA_DATA_ROOT", os.path.join(ROOT, "SEA-DATA"))
DB_DIR = os.environ.get("ATLAS_DB_DIR") or ROOT
PFP_DB = os.environ.get("PFP_DB") or os.path.join(DB_DIR, "pfp.duckdb")

NPOS = 560
FRAME_MS = 10.0
QMIN, QMAX = -130.0, -10.0   # int8 range (dBm), ~0.47 dB/step

# PFP_<YYYY-MM-DD>_<sensor>_<stat>.csv , where stat may itself contain one
# underscore (max_peak, mean_rms). The lazy sensor match resolves the rest.
NAME_RE = re.compile(r"^PFP_(?P<day>\d{4}-\d{2}-\d{2})_(?P<sensor>.+?)_"
                     r"(?P<stat>[A-Za-z0-9]+(?:_[A-Za-z0-9]+)?)\.csv$")
PATTERN = "PFP_<YYYY-MM-DD>_<sensor>_<stat>.csv"

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def discover(root, stat=None):
    return cbrs_files.discover(root, NAME_RE, stat)


def stats_present(root):
    return cbrs_files.stats_present(root, NAME_RE)


def read_frames(path, csv_connection):
    """CSV -> (epoch_seconds[n], freq_hz[n], quantized uint8 [n,560])."""
    ts, keys, Q = cbrs_files.read_quantized(
        path, csv_connection, NPOS, QMIN, QMAX, "dBm", "frame-position",
        lead=("frequency",))
    return ts, keys["frequency"], Q


def main():
    ap = argparse.ArgumentParser(
        description="Ingest CBRS PFP CSVs into pfp.duckdb.")
    ap.add_argument("sensor", nargs="?", default=None,
                    help="sensor name; omit when only one is on disk")
    ap.add_argument("--root", default=DATA_ROOT,
                    help=f"directory holding the source CSVs (default {DATA_ROOT})")
    ap.add_argument("--stat", default="max_peak",
                    help="which statistic to ingest (default max_peak)")
    ap.add_argument("--limit", type=int, default=None,
                    help="only the first N days, for a quick test")
    ap.add_argument("--list", action="store_true",
                    help="show the sensors and day counts found, then exit")
    args = ap.parse_args()

    root = args.root
    if not os.path.isdir(root):
        sys.exit(cbrs_files.missing_root(root))

    found = discover(root, args.stat)
    if not found:
        sys.exit(cbrs_files.no_data(root, args.stat, NAME_RE,
                                    PATTERN, "PFP"))

    if args.list:
        print(f"{os.path.abspath(root)} (stat: {args.stat})\n")
        for s in sorted(found):
            days = sorted(found[s])
            print(f"  {s:24} {len(days):>5} day(s)  {days[0]} .. {days[-1]}")
        print(f"\nstats available: {', '.join(stats_present(root))}")
        print("\nnext: python ingest/pfp_ingest.py <sensor>")
        return 0

    sensor, why = cbrs_files.resolve_sensor(args.sensor, found, root)
    if why:
        sys.exit(why)
    if args.sensor is None:
        print(f"one sensor on disk; ingesting {sensor}")

    by_day = found[sensor]
    days = sorted(by_day)
    print(f"{sensor} PFP ({args.stat}): {len(days)} day(s) on disk")

    connection = duckdb.connect(PFP_DB)
    connection.execute("CREATE TABLE IF NOT EXISTS pfp (sensor VARCHAR, freq DOUBLE, t DOUBLE, frame BLOB)")
    connection.execute("""CREATE TABLE IF NOT EXISTS pfp_meta (
        sensor VARCHAR, stat VARCHAR, npos INT, frame_ms DOUBLE, qmin DOUBLE, qmax DOUBLE,
        t_min DOUBLE, t_max DOUBLE, rows BIGINT)""")
    # This database may already be compacted -- see the note in psd_ingest.py.
    # Append in whichever shape is on disk, so a second run months later lands
    # where serve.py will actually read it.
    kind = chunk_io.schema_of(connection, "pfp")
    if kind == "chunk":
        print("  this database is compacted; appending new days as chunks")
    # Resumable: skip days already ingested, in UTC on both sides.
    # to_timestamp() renders in the machine's local zone while the filename's
    # day is UTC, so west of Greenwich an early-UTC-morning day never matches
    # its own filename and is re-read and re-inserted on every resume. And count
    # the days this run is actually skipping rather than every day the sensor has
    # in the table -- `existing` spans days that are not under this --root at
    # all, which is how "1 day(s) on disk / 2 day(s) already ingested" came about.
    # Tested against each file's OWN first capture, not the day its name
    # carries: a CBRS export runs past the next midnight, so the day-name test
    # marked day D+1 complete as soon as day D was read and then skipped D+1
    # whole. Falls back to the day test when a stamp will not parse, since the
    # append has no per-day delete and a wrong "not ingested" duplicates rows.
    stored_ms = {int(round(t * 1000)) for t in
                 chunk_io.stored_times(connection, "pfp", sensor, kind)}
    existing = chunk_io.existing_days(connection, "pfp", sensor, kind)

    def have(d):
        first = cbrs_files.first_capture_time(by_day[d])
        return (d in existing) if first is None else \
            chunk_io.already_have(stored_ms, first)

    skipping = sum(1 for d in days if have(d))
    if skipping:
        print(f"  resuming: {skipping} of {len(days)} day(s) already "
              f"ingested, skipping those")
    csv_connection = duckdb.connect()

    t0 = time.time()
    done = err = nrows = 0
    errors = []
    # A chunk is one channel's consecutive frames, so the appenders are keyed by
    # frequency and kept open across days -- a day holds only a handful of frames
    # per channel, and closing per day would leave a chunk table of stubs.
    apps = {}
    for i, day in enumerate(days, 1):
        if have(day):
            done += 1
            continue
        path = by_day[day]
        try:
            ts, freq, Q = read_frames(path, csv_connection)
            if kind == "chunk":
                for f in np.unique(np.asarray(freq, dtype=np.float64)):
                    sel = np.flatnonzero(np.asarray(freq, dtype=np.float64) == f)
                    app = apps.get(float(f))
                    if app is None:
                        app = apps[float(f)] = chunk_io.ChunkAppender(
                            connection, "pfp", sensor, "frames", key=float(f))
                    app.add([ts[j] for j in sel], [Q[j] for j in sel])
            else:
                connection.executemany(
                    "INSERT INTO pfp VALUES (?,?,?,?)",
                    [(sensor, float(freq[j]), float(ts[j]), Q[j].tobytes())
                     for j in range(len(ts))])
            done += 1; nrows += len(ts)
        except Exception as e:
            err += 1
            errors.append(f"{os.path.basename(path)}: {e}")
            print(f"  ERR {os.path.basename(path)}: {e}")
        if i % 10 == 0 or i == len(days):
            print(f"  [{i}/{len(days)}] rows={nrows:,} done={done} err={err} "
                  f"{i/max(time.time()-t0,1e-6):.2f} days/s")
    for app in apps.values():
        app.flush()

    r = chunk_io.stored_span(connection, "pfp", sensor, chunk_io.schema_of(connection, "pfp"))
    total = r[2] or 0
    if total == 0:
        connection.execute("DELETE FROM pfp_meta WHERE sensor=?", [sensor])
        connection.close()
        print(f"\nNothing ingested for {sensor}: every one of the {len(days)} "
              f"file(s) failed to read.", file=sys.stderr)
        for e in errors[:5]:
            print(f"  {e}", file=sys.stderr)
        return 1
    connection.execute("DELETE FROM pfp_meta WHERE sensor=?", [sensor])
    connection.execute("INSERT INTO pfp_meta VALUES (?,?,?,?,?,?,?,?,?)",
                [sensor, args.stat, NPOS, FRAME_MS, QMIN, QMAX, r[0], r[1], total])
    connection.close()
    print(f"\nDone in {(time.time()-t0)/60:.1f} min. {nrows:,} new frames, "
          f"{total:,} total. DB = {os.path.getsize(PFP_DB)/1e9:.2f} GB")
    if err:
        print(f"{err} file(s) failed to read; re-run to retry them.",
              file=sys.stderr)
    print("next: python serve.py     (optional: python ingest/compact_db.py "
          "first, to shrink the file)")
    return 1 if err and nrows == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
