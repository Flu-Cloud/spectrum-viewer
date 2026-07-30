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
size and speed, not whether the viewer works.

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

import duckdb
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cbrs_files                                    # noqa: E402

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


def read_specs(path, rcon):
    """CSV -> (epoch_seconds[n], quantized uint8 [n,2250])."""
    res = rcon.execute("SELECT * FROM read_csv_auto(?, header=true)", [path])
    cols = [c[0] for c in res.description]
    if len(cols) - 1 != NF:
        raise ValueError(f"expected {NF} spectrum columns after the timestamp, "
                         f"found {len(cols) - 1}. This file is not a "
                         f"{NF}-bin CBRS PSD export.")
    d = res.fetchnumpy()
    ts = d[cols[0]].astype("datetime64[us]").astype("int64") / 1e6   # epoch sec
    P = np.stack([np.asarray(d[c], dtype=np.float64) for c in cols[1:]], axis=1)
    Q = np.clip(np.round((P - QMIN) / (QMAX - QMIN) * 255.0), 0, 255).astype(np.uint8)
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

    sensor, why = cbrs_files.resolve_sensor(args.sensor, found, root)
    if why:
        sys.exit(why)
    if args.sensor is None:
        print(f"one sensor on disk; ingesting {sensor}")

    by_day = found[sensor]
    days = sorted(by_day)
    if args.limit:
        days = days[:args.limit]
    print(f"{sensor}: {len(days)} day(s) on disk (compact int8 BLOB)")

    con = duckdb.connect(PSD_DB)
    con.execute("CREATE TABLE IF NOT EXISTS psd (sensor VARCHAR, t DOUBLE, spec BLOB)")
    con.execute("""CREATE TABLE IF NOT EXISTS psd_meta (
        sensor VARCHAR, f0 DOUBLE, df DOUBLE, nf INT, qmin DOUBLE, qmax DOUBLE,
        t_min DOUBLE, t_max DOUBLE, captures BIGINT)""")
    # resumable: skip days already ingested
    existing = set(str(r[0]) for r in con.execute(
        "SELECT DISTINCT CAST(to_timestamp(t) AS DATE) FROM psd WHERE sensor=?",
        [sensor]).fetchall())
    if existing:
        print(f"  resuming: {len(existing)} day(s) already ingested, skipping those")
    rcon = duckdb.connect()   # separate handle for CSV reads

    t0 = time.time()
    done = err = caps = 0
    errors = []
    for i, day in enumerate(days, 1):
        if day in existing:
            done += 1
            continue
        path = by_day[day]
        try:
            ts, Q = read_specs(path, rcon)
            con.executemany(
                "INSERT INTO psd VALUES (?,?,?)",
                [(sensor, float(ts[j]), Q[j].tobytes()) for j in range(len(ts))])
            done += 1; caps += len(ts)
        except Exception as e:
            err += 1
            errors.append(f"{os.path.basename(path)}: {e}")
            print(f"  ERR {os.path.basename(path)}: {e}")
        if i % 25 == 0 or i == len(days):
            rate = i / max(time.time() - t0, 1e-6)
            print(f"  [{i}/{len(days)}] caps={caps:,} done={done} err={err} "
                  f"{rate:.1f} days/s")

    rng = con.execute("SELECT min(t), max(t), count(*) FROM psd WHERE sensor=?",
                      [sensor]).fetchone()
    total = rng[2] or 0
    if total == 0:
        # Never leave a meta row claiming a sensor that has no spectra: the
        # server would advertise the layer and then have nothing to draw.
        con.execute("DELETE FROM psd_meta WHERE sensor=?", [sensor])
        con.close()
        print(f"\nNothing ingested for {sensor}: every one of the {len(days)} "
              f"file(s) failed to read.", file=sys.stderr)
        for e in errors[:5]:
            print(f"  {e}", file=sys.stderr)
        return 1
    con.execute("DELETE FROM psd_meta WHERE sensor=?", [sensor])
    con.execute("INSERT INTO psd_meta VALUES (?,?,?,?,?,?,?,?,?)",
                [sensor, F0, DF, NF, QMIN, QMAX, rng[0], rng[1], total])
    con.close()
    size = os.path.getsize(PSD_DB) / 1e9
    print(f"\nDone in {(time.time()-t0)/60:.1f} min. {sensor}: {caps:,} new "
          f"captures, {total:,} total. DB {PSD_DB} = {size:.2f} GB (all sensors)")
    if err:
        print(f"{err} file(s) failed to read; re-run to retry them.",
              file=sys.stderr)
    print("next: python serve.py     (optional: python ingest/compact_db.py "
          "first, to shrink the file)")
    return 1 if err and caps == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
