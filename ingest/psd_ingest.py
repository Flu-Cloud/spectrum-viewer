"""
psd_ingest.py: ingest PSD numbers into a compact, fast store.

Each capture's 2250-bin spectrum is quantized to int8 (0.35 dB steps over
[-180,-90] dBm/Hz) and stored as one BLOB row: (sensor, t, spec). That's ~1
row per capture (~180k) instead of 398M long-form rows, so ingest skips the
expensive UNPIVOT, the DB is ~4x smaller, and rendering is just a fetch + numpy
bin. Re-render at any frequency zoom stays crisp (the numbers are preserved).

Source CSVs are found by walking --root (default $SEA_DATA_ROOT, else
./SEA-DATA) and matching <YYYY-MM-DD>_<sensor>_<stat>.csv anywhere underneath,
so it does not matter whether they sit in a PSD/ folder, in the layout
fetch.py produced, or loose in one directory.

The server reads this file directly. compact_db.py repacks it into a smaller
chunked form and swaps that in; that step is now optional and only affects
size and speed, not whether the viewer works. Re-running against a database
that is already compacted appends in the chunked shape rather than the row
shape, so next month's data lands where the server will read it.

    py psd_ingest.py --list                        # what is on disk
    py psd_ingest.py CBBT-Directional              # one sensor
    py psd_ingest.py CBBT-Directional --limit 5    # quick test
    py psd_ingest.py --root /path/to/SEA-DATA      # explicit source directory
"""

import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _require  # noqa: F401  -- deps message instead of a traceback

import duckdb

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
PSD_DB = os.environ.get("PSD_DB") or os.path.join(DB_DIR, "psd.duckdb")

F0 = 3530040000.0    # first PSD bin (Hz)
DF = 80000.0         # bin spacing (Hz)
NF = 2250            # bins
QMIN, QMAX = -180.0, -90.0   # int8 quantization range (dBm/Hz)

# <YYYY-MM-DD>_<sensor>_<stat>.csv . The sensor match is lazy and the stat has
# no underscore, so sensor names containing underscores still resolve.
NAME_RE = re.compile(r"^(?P<day>\d{4}-\d{2}-\d{2})_(?P<sensor>.+?)_"
                     r"(?P<stat>[A-Za-z0-9]+)\.csv$")
PATTERN = "<YYYY-MM-DD>_<sensor>_<stat>.csv"

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def discover(root, stat=None):
    return cbrs_files.discover(root, NAME_RE, stat)


def stats_present(root):
    return cbrs_files.stats_present(root, NAME_RE)


def read_specs(path, csv_connection):
    """CSV -> (epoch_seconds[n], quantized uint8 [n,2250])."""
    ts, _keys, Q = cbrs_files.read_quantized(
        path, csv_connection, NF, QMIN, QMAX, "dBm/Hz", "spectrum")
    return ts, Q


def main():
    ap = argparse.ArgumentParser(
        description="Ingest CBRS PSD CSVs into psd.duckdb.")
    ap.add_argument("sensor", nargs="?", default=None,
                    help="sensor name; omit when only one is on disk")
    ap.add_argument("--root", default=DATA_ROOT,
                    help=f"directory holding the source CSVs (default {DATA_ROOT})")
    ap.add_argument("--stat", default="max",
                    help="which statistic to ingest (default max)")
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
                                    PATTERN, "PSD"))

    if args.list:
        print(f"{os.path.abspath(root)} (stat: {args.stat})\n")
        for s in sorted(found):
            days = sorted(found[s])
            print(f"  {s:24} {len(days):>5} day(s)  {days[0]} .. {days[-1]}")
        print(f"\nstats available: {', '.join(stats_present(root))}")
        print("\nnext: python ingest/psd_ingest.py <sensor>")
        return 0

    # Each statistic gets its own database, next to the max one: psd.duckdb for
    # max (the path that always existed, unchanged), psd_median.duckdb and
    # psd_mean.duckdb for the others. Separate files rather than a schema
    # change, so every existing database keeps working untouched and serve.py
    # simply offers whichever statistics have been ingested.
    global PSD_DB
    if args.stat != "max":
        base_dir = os.path.dirname(PSD_DB) or "."
        PSD_DB = os.path.join(base_dir, f"psd_{args.stat}.duckdb")
        print(f"stat '{args.stat}' -> {os.path.basename(PSD_DB)}")

    sensor, why = cbrs_files.resolve_sensor(args.sensor, found, root)
    if why:
        sys.exit(why)
    if args.sensor is None:
        print(f"one sensor on disk; ingesting {sensor}")

    by_day = found[sensor]
    days = sorted(by_day)
    print(f"{sensor}: {len(days)} day(s) on disk (compact int8 BLOB)")

    connection = duckdb.connect(PSD_DB)
    connection.execute("CREATE TABLE IF NOT EXISTS psd (sensor VARCHAR, t DOUBLE, spec BLOB)")
    connection.execute("""CREATE TABLE IF NOT EXISTS psd_meta (
        sensor VARCHAR, f0 DOUBLE, df DOUBLE, nf INT, qmin DOUBLE, qmax DOUBLE,
        t_min DOUBLE, t_max DOUBLE, captures BIGINT)""")
    # This database may already be compacted: the datasets grow, so running the
    # ingest again next month usually means appending to a file compact_db.py
    # has rewritten into chunks. Write in whichever shape is already there --
    # writing rows into a compacted file leaves both schemas in it, and
    # serve.py, preferring the chunk table, would never draw the new month.
    kind = chunk_io.schema_of(connection, "psd")
    if kind == "chunk":
        print("  this database is compacted; appending new days as chunks")
    # Resumable: skip days already ingested, in UTC on both sides. t is the
    # epoch second of a UTC capture and the filename's day is that same UTC day,
    # but to_timestamp() renders in the machine's local zone -- west of
    # Greenwich a 00:30 UTC capture buckets to the previous date. A day whose
    # captures are all early UTC morning then never matches its own filename, so
    # it is re-read and inserted again on every resume (duplicate rows), while a
    # day that does match can be reported skipped without ever being compared.
    existing = chunk_io.existing_days(connection, "psd", sensor, kind)
    todo = [d for d in days if d not in existing]
    # Count the days this run is actually skipping, not every day the sensor
    # has in the DB: `existing` spans the whole table, including days that are
    # not under this --root at all. Reporting len(existing) is how "1 day on
    # disk / 2 day(s) already ingested" -- more skipped than found -- happened.
    if len(todo) < len(days):
        print(f"  resuming: {len(days) - len(todo)} of {len(days)} day(s) "
              f"already ingested, skipping those")
    # --limit caps the days this run reads, applied after the resume filter.
    # Capping `days` first re-selects the same already-ingested days on every
    # run, so a limited resume can never advance past the first N days.
    if args.limit:
        todo = todo[:args.limit]
    csv_connection = duckdb.connect()   # separate handle for CSV reads

    t0 = time.time()
    done = err = caps = 0
    errors = []
    app = (chunk_io.ChunkAppender(connection, "psd", sensor, "specs")
           if kind == "chunk" else None)
    for i, day in enumerate(todo, 1):
        path = by_day[day]
        try:
            ts, Q = read_specs(path, csv_connection)
            if app is not None:
                app.add(ts, [Q[j] for j in range(len(ts))])
            else:
                connection.executemany(
                    "INSERT INTO psd VALUES (?,?,?)",
                    [(sensor, float(ts[j]), Q[j].tobytes()) for j in range(len(ts))])
            done += 1; caps += len(ts)
        except Exception as e:
            err += 1
            errors.append(f"{os.path.basename(path)}: {e}")
            print(f"  ERR {os.path.basename(path)}: {e}")
        if i % 25 == 0 or i == len(todo):
            rate = i / max(time.time() - t0, 1e-6)
            print(f"  [{i}/{len(todo)}] caps={caps:,} done={done} err={err} "
                  f"{rate:.1f} days/s")
    if app is not None:
        app.flush()

    rng = chunk_io.stored_span(connection, "psd", sensor,
                               chunk_io.schema_of(connection, "psd"))
    total = rng[2] or 0
    if total == 0:
        # Never leave a meta row claiming a sensor that has no spectra: the
        # server would advertise the layer and then have nothing to draw.
        connection.execute("DELETE FROM psd_meta WHERE sensor=?", [sensor])
        connection.close()
        print(f"\nNothing ingested for {sensor}: every one of the {len(todo)} "
              f"file(s) failed to read.", file=sys.stderr)
        for e in errors[:5]:
            print(f"  {e}", file=sys.stderr)
        return 1
    connection.execute("DELETE FROM psd_meta WHERE sensor=?", [sensor])
    connection.execute("INSERT INTO psd_meta VALUES (?,?,?,?,?,?,?,?,?)",
                [sensor, F0, DF, NF, QMIN, QMAX, rng[0], rng[1], total])
    connection.close()
    size = os.path.getsize(PSD_DB) / 1e9
    print(f"\nDone in {(time.time()-t0)/60:.1f} min. {sensor}: {caps:,} new "
          f"captures, {total:,} total. DB {PSD_DB} = {size:.2f} GB (all sensors)")
    if err:
        print(f"{err} file(s) failed to read; re-run to retry them.",
              file=sys.stderr)
    print("next: python serve.py     (optional: python ingest/compact_db.py "
          "first, to shrink the file)")
    # Any unread file means this sensor is missing days, so exit non-zero even
    # when other files succeeded. `err and caps == 0` reported success for a
    # run that dropped half its input, and atlas.py and make_sample.py branch
    # on this to decide whether the step is done.
    return 1 if err else 0


if __name__ == "__main__":
    sys.exit(main())
