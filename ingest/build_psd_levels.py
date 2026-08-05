"""build_psd_levels.py: a coarse-time pyramid for the PSD layer.

The PSD layer stores one spectrum per capture. Drawing a WIDE time window from
that means reading and zlib-inflating every chunk in range -- on a 200-day
window that is hundreds of megabytes of inflation to fill a few thousand pixel
columns, and it measured 18 s. serve.py already thinned the captures it kept
(one in eight at that width), so nearly all of that inflation was thrown away.

This builds what the summary layer has always had: max spectra over fixed time
buckets, so a wide window reads a few thousand small rows instead.

    psd_lvl(sensor, bucket, t, smax)     smax = 2250 uint8 bytes, the bin-wise
                                         max over [t, t + bucket)

Two things worth being precise about:

  * It is not a loss of fidelity at those widths -- it is a gain. A column that
    used to be the max over one capture in eight is now the max over EVERY
    capture in the bucket. Closer to the max the data actually holds, not
    further from it.
  * `max` is what makes this sound. Max is associative, so the 6 h level is the
    bin-wise max of six 1 h rows and the 1 d level the max of four 6 h rows --
    each derived exactly from the one below it, no re-reading. A mean or median
    layer could NOT be built this way.

Additive and resumable: it only ever creates and fills the psd_lvl table,
records finished sensors, and leaves every existing table untouched. Stop
serve.py first -- DuckDB allows one writer at a time.

    py build_psd_levels.py
    py build_psd_levels.py --sensor HU
"""
import argparse
import os
import sys
import time
import zlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _require  # noqa: F401  -- deps message instead of a traceback

import duckdb
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DB_DIR = os.environ.get("ATLAS_DB_DIR") or ROOT
PSD_DB = os.environ.get("PSD_DB") or os.path.join(DB_DIR, "psd.duckdb")

NF = 2250
# Finest bucket first; each coarser one an exact multiple of the one before, so
# it can be derived from it by max instead of re-read. serve.py only reaches for
# a level once a pixel column is at least one bucket wide, so the finest level
# sets how narrow a window still gets the speed-up: 10 min covers spans past
# ~7 days on a 1300-column plot, 1 h only past ~40 days. Below that the captures
# are already sub-500 ms and are read unchanged.
#
# The 10 min level is the bulk of the size (~65 MB per sensor-200-days, against
# ~11 MB for all three coarser ones together). --coarse skips it if the disk
# matters more than mid-range zoom.
BASE = 600
DERIVED = [3600, 21600, 86400]
LEVELS = [BASE] + DERIVED

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def _tables(con):
    return {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables").fetchall()}


def _iter_captures(con, sensor, kind):
    """(times, spectra) batches for one sensor, whichever schema is on disk."""
    if kind == "chunk":
        cur = con.execute("SELECT n, times, specs FROM psd_chunk WHERE sensor=? "
                          "ORDER BY t0", [sensor])
        while True:
            rows = cur.fetchmany(64)
            if not rows:
                return
            for n, tb, sb in rows:
                ts = np.frombuffer(zlib.decompress(tb), np.float64)
                mat = np.frombuffer(zlib.decompress(sb), np.uint8).reshape(n, NF)
                yield ts, mat
    else:
        cur = con.execute("SELECT t, spec FROM psd WHERE sensor=? ORDER BY t",
                          [sensor])
        while True:
            rows = cur.fetchmany(4096)
            if not rows:
                return
            ts = np.array([r[0] for r in rows], np.float64)
            mat = np.stack([np.frombuffer(r[1], np.uint8) for r in rows])
            yield ts, mat


def build_sensor(con, sensor, kind):
    """-> number of level rows written for this sensor."""
    t_start = time.time()
    # Bin-wise max per BASE bucket, accumulated in memory. One bucket is 2250
    # bytes; a year at 1 h is 8760 of them, under 20 MB, so a dict is fine and
    # avoids a second pass over the captures.
    buckets = {}
    ncap = 0
    for ts, mat in _iter_captures(con, sensor, kind):
        ncap += len(ts)
        keys = (ts // BASE).astype(np.int64)
        for k in np.unique(keys):
            sel = mat[keys == k]
            m = sel.max(axis=0)
            prev = buckets.get(k)
            buckets[k] = m if prev is None else np.maximum(prev, m)
    if not buckets:
        return 0

    written = 0
    con.execute("DELETE FROM psd_lvl WHERE sensor=?", [sensor])   # partial redo
    cur_level = {k * BASE: v for k, v in buckets.items()}
    con.executemany("INSERT INTO psd_lvl VALUES (?,?,?,?)",
                    [(sensor, BASE, int(t), v.tobytes())
                     for t, v in sorted(cur_level.items())])
    written += len(cur_level)

    # Coarser levels by exact max of the level below -- no re-reading.
    prev_bucket = BASE
    for bucket in DERIVED:
        assert bucket % prev_bucket == 0, "levels must nest exactly"
        nxt = {}
        for t, v in cur_level.items():
            k = (t // bucket) * bucket
            cur = nxt.get(k)
            nxt[k] = v if cur is None else np.maximum(cur, v)
        con.executemany("INSERT INTO psd_lvl VALUES (?,?,?,?)",
                        [(sensor, bucket, int(t), v.tobytes())
                         for t, v in sorted(nxt.items())])
        written += len(nxt)
        cur_level, prev_bucket = nxt, bucket

    log(f"  {sensor}: {ncap:,} captures -> {written:,} level rows "
        f"in {time.time()-t_start:.0f}s")
    return written


def main():
    global BASE, DERIVED, LEVELS
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sensor", default=None, help="one sensor (default: all)")
    ap.add_argument("--rebuild", action="store_true",
                    help="redo sensors already built")
    ap.add_argument("--coarse", action="store_true",
                    help="skip the finest (10 min) level: much smaller on disk, "
                         "but mid-range spans then read the captures as before")
    args = ap.parse_args()

    if args.coarse:
        BASE, DERIVED = DERIVED[0], DERIVED[1:]
        LEVELS = [BASE] + DERIVED

    if not os.path.exists(PSD_DB):
        sys.exit(f"{PSD_DB} not found. Build it with ingest/psd_ingest.py first.")
    try:
        con = duckdb.connect(PSD_DB)
    except Exception as e:
        sys.exit(f"cannot open {os.path.basename(PSD_DB)} for writing: {e}\n"
                 "  Stop serve.py (and any other reader) and run this again -- "
                 "DuckDB allows one writer at a time.")

    have = _tables(con)
    kind = ("chunk" if "psd_chunk" in have
            else "rows" if "psd" in have else None)
    if kind is None:
        sys.exit(f"{os.path.basename(PSD_DB)} has no PSD table to summarise.")

    con.execute("""CREATE TABLE IF NOT EXISTS psd_lvl (
        sensor VARCHAR, bucket INTEGER, t BIGINT, smax BLOB)""")
    con.execute("CREATE TABLE IF NOT EXISTS psd_lvl_done (sensor VARCHAR)")

    src = "psd_chunk" if kind == "chunk" else "psd"
    sensors = [r[0] for r in con.execute(
        f"SELECT DISTINCT sensor FROM {src} ORDER BY 1").fetchall()]
    if args.sensor:
        if args.sensor not in sensors:
            sys.exit(f"no PSD for sensor '{args.sensor}'. Have: "
                     + ", ".join(sensors))
        sensors = [args.sensor]
    if args.rebuild:
        con.execute("DELETE FROM psd_lvl_done"
                    + ("" if args.sensor is None else " WHERE sensor=?"),
                    [] if args.sensor is None else [args.sensor])

    done = {r[0] for r in con.execute("SELECT sensor FROM psd_lvl_done").fetchall()}
    log(f"{os.path.basename(PSD_DB)} ({kind} schema): {len(sensors)} sensor(s), "
        f"levels {', '.join(str(b) for b in LEVELS)}s")
    total = 0
    for s in sensors:
        if s in done:
            log(f"  {s}: already built (--rebuild to redo)")
            continue
        n = build_sensor(con, s, kind)
        if n == 0:
            log(f"  {s}: no captures, skipped")
            continue
        con.execute("INSERT INTO psd_lvl_done VALUES (?)", [s])
        total += n
    rows = con.execute("SELECT count(*) FROM psd_lvl").fetchone()[0]
    con.close()
    log(f"DONE: {total:,} row(s) added this run; psd_lvl holds {rows:,}. "
        f"DB is now {os.path.getsize(PSD_DB)/1e9:.2f} GB")
    log("serve.py picks the levels up automatically on its next start.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
