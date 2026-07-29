"""
pfp_ingest.py: ingest PFP (periodic-frame-power) numbers into a compact store.

PFP = power across a 10 ms frame (560 positions, ~17.86 us each) per 10 MHz
channel, one trace per ~4-min capture. We store one int8-quantized BLOB per
(channel, capture): (sensor, freq, t, frame). Re-render any channel/time window
crisply, like the PSD layer.

Source CSVs are found by walking --root (default $SEA_DATA_ROOT, else
./SEA-DATA) and matching PFP_<YYYY-MM-DD>_<sensor>_<stat>.csv anywhere
underneath, so the folder layout does not matter.

The server reads this file directly. compact_db.py repacks it into a smaller
chunked form and swaps that in; that step is now optional and only affects
size and speed, not whether the viewer works.

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

import duckdb
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)   # repo root (scripts live in ingest/)
# Where the CBRS source CSVs live. Set SEA_DATA_ROOT to your copy, or pass
# --root; defaults to ./SEA-DATA in the repo so a fresh clone never points at
# someone else's disk.
DATA_ROOT = os.environ.get("SEA_DATA_ROOT", os.path.join(ROOT, "SEA-DATA"))
PFP_DB = os.environ.get("PFP_DB", os.path.join(ROOT, "pfp.duckdb"))

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
    """Walk root -> {sensor: {day: path}} for matching PFP CSVs, any layout."""
    found = {}
    for dirpath, _, names in os.walk(root):
        for n in names:
            m = NAME_RE.match(n)
            if not m or (stat and m.group("stat") != stat):
                continue
            found.setdefault(m.group("sensor"), {})[m.group("day")] = \
                os.path.join(dirpath, n)
    return found


def stats_present(root):
    out = set()
    for _, _, names in os.walk(root):
        for n in names:
            m = NAME_RE.match(n)
            if m:
                out.add(m.group("stat"))
    return sorted(out)


def sample_of(root, limit=8):
    out = []
    for dirpath, _, names in os.walk(root):
        for n in sorted(names)[:limit]:
            out.append(os.path.relpath(os.path.join(dirpath, n), root))
            if len(out) >= limit:
                return out
    return out


def no_data(root, stat):
    print(f"No PFP CSVs found under {os.path.abspath(root)}", file=sys.stderr)
    print(f"  looked for: {PATTERN}"
          + (f" with stat '{stat}'" if stat else "") + " (searched recursively)",
          file=sys.stderr)
    other = stats_present(root)
    if stat and other:
        print(f"  files with that name shape exist, but their stats are: "
              f"{', '.join(other)}. Try --stat {other[0]}.", file=sys.stderr)
    else:
        found = sample_of(root)
        if found:
            print("  what is actually there:", file=sys.stderr)
            for f in found:
                print(f"    {f}", file=sys.stderr)
        else:
            print("  that directory is empty.", file=sys.stderr)
    print("  Point --root at your copy of the data, or set SEA_DATA_ROOT "
          "(PowerShell: $env:SEA_DATA_ROOT=\"...\").", file=sys.stderr)
    return 1


def read_frames(path, rcon):
    """CSV -> (epoch_seconds[n], freq_hz[n], quantized uint8 [n,560])."""
    res = rcon.execute("SELECT * FROM read_csv_auto(?, header=true)", [path])
    cols = [c[0] for c in res.description]              # datetime, frequency, 0..559
    if len(cols) - 2 != NPOS:
        raise ValueError(f"expected {NPOS} frame-position columns after "
                         f"timestamp and frequency, found {len(cols) - 2}. "
                         "This file is not a 560-position CBRS PFP export.")
    d = res.fetchnumpy()
    ts = d[cols[0]].astype("datetime64[us]").astype("int64") / 1e6
    freq = np.asarray(d[cols[1]], dtype=np.float64)
    pos_cols = cols[2:]
    P = np.stack([np.asarray(d[c], dtype=np.float64) for c in pos_cols], axis=1)   # (n,560)
    Q = np.clip(np.round((P - QMIN) / (QMAX - QMIN) * 255.0), 0, 255).astype(np.uint8)
    return ts, freq, Q


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
        sys.exit(f"source directory not found: {os.path.abspath(root)}\n"
                 "  Pass --root /path/to/your/data, or set SEA_DATA_ROOT "
                 "(PowerShell: $env:SEA_DATA_ROOT=\"...\").\n"
                 "  To download it: python ingest/fetch.py <record-id> "
                 "--dest <that directory>")

    found = discover(root, args.stat)
    if not found:
        sys.exit(no_data(root, args.stat))

    if args.list:
        print(f"{os.path.abspath(root)} (stat: {args.stat})\n")
        for s in sorted(found):
            days = sorted(found[s])
            print(f"  {s:24} {len(days):>5} day(s)  {days[0]} .. {days[-1]}")
        print(f"\nstats available: {', '.join(stats_present(root))}")
        print("\nnext: python ingest/pfp_ingest.py <sensor>")
        return 0

    sensor = args.sensor
    if sensor is None:
        if len(found) == 1:
            sensor = next(iter(found))
            print(f"one sensor on disk; ingesting {sensor}")
        else:
            sys.exit(f"which sensor? {len(found)} found under "
                     f"{os.path.abspath(root)}:\n  "
                     + "\n  ".join(sorted(found))
                     + "\n  Pass one as the first argument.")
    if sensor not in found:
        near = [s for s in found if s.lower() == sensor.lower()]
        hint = f"\n  Did you mean: {near[0]}" if near else \
            "\n  Available: " + ", ".join(sorted(found))
        sys.exit(f"no PFP CSVs for sensor '{sensor}' under "
                 f"{os.path.abspath(root)}." + hint)

    by_day = found[sensor]
    days = sorted(by_day)
    if args.limit:
        days = days[:args.limit]
    print(f"{sensor} PFP ({args.stat}): {len(days)} day(s) on disk")

    con = duckdb.connect(PFP_DB)
    con.execute("CREATE TABLE IF NOT EXISTS pfp (sensor VARCHAR, freq DOUBLE, t DOUBLE, frame BLOB)")
    con.execute("""CREATE TABLE IF NOT EXISTS pfp_meta (
        sensor VARCHAR, stat VARCHAR, npos INT, frame_ms DOUBLE, qmin DOUBLE, qmax DOUBLE,
        t_min DOUBLE, t_max DOUBLE, rows BIGINT)""")
    # resumable: skip days already ingested, so a long pull can restart
    existing = set(str(r[0]) for r in con.execute(
        "SELECT DISTINCT CAST(to_timestamp(t) AS DATE) FROM pfp WHERE sensor=?",
        [sensor]).fetchall())
    if existing:
        print(f"  resuming: {len(existing)} day(s) already ingested, skipping those")
    rcon = duckdb.connect()

    t0 = time.time()
    done = err = nrows = 0
    errors = []
    for i, day in enumerate(days, 1):
        if day in existing:
            done += 1
            continue
        path = by_day[day]
        try:
            ts, freq, Q = read_frames(path, rcon)
            con.executemany(
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

    r = con.execute("SELECT min(t), max(t), count(*) FROM pfp WHERE sensor=?",
                    [sensor]).fetchone()
    total = r[2] or 0
    if total == 0:
        con.execute("DELETE FROM pfp_meta WHERE sensor=?", [sensor])
        con.close()
        print(f"\nNothing ingested for {sensor}: every one of the {len(days)} "
              f"file(s) failed to read.", file=sys.stderr)
        for e in errors[:5]:
            print(f"  {e}", file=sys.stderr)
        return 1
    con.execute("DELETE FROM pfp_meta WHERE sensor=?", [sensor])
    con.execute("INSERT INTO pfp_meta VALUES (?,?,?,?,?,?,?,?,?)",
                [sensor, args.stat, NPOS, FRAME_MS, QMIN, QMAX, r[0], r[1], total])
    con.close()
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
