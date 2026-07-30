"""compact_db.py: repack the live DBs into much smaller files (lossless).

  spectrum.duckdb : dBm DOUBLE -> SMALLINT (dBm*10), t -> BIGINT    ~1.9 -> ~0.7 GB
  psd.duckdb      : per-capture rows -> zlib chunks of 256 spectra  ~5.4 -> ~3.3 GB
  pfp.duckdb      : per-capture rows -> zlib chunks of 1024 frames  ~14.3 -> ~5.5 GB
                    (consecutive same-channel frames compress 3.1x; measured)

Writes *_c.duckdb build files next to the originals, then moves each one into
place, keeping the original as <name>.duckdb.bak. Pass --no-swap to skip that
last step. Resumable: finished units are recorded in a `done` table, a crashed
partial unit is deleted and redone.

This step is optional. serve.py reads the uncompacted files the ingest scripts
write, so compaction only makes them smaller and faster.

    py compact_db.py
    py compact_db.py --no-swap
"""
import argparse
import os
import sys
import time
import zlib

import duckdb
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)   # repo root (scripts live in ingest/)
# Where the .duckdb files live; override to compact a copy elsewhere.
DB_DIR = os.environ.get("ATLAS_DB_DIR") or ROOT
Z = 6                # zlib level (PFP 3.1x, PSD 1.7x at z6; measured on live data)
PSD_CHUNK = 256      # spectra per chunk  (256*2250 = 576 KB raw)
PFP_CHUNK = 1024     # frames per chunk   (1024*560 = 573 KB raw)

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _prep(dst, ddl):
    dst.execute("CREATE TABLE IF NOT EXISTS done (k VARCHAR)")
    dst.execute(ddl)


def _skip(dst, key):
    return dst.execute("SELECT 1 FROM done WHERE k=?", [key]).fetchone() is not None


def _missing(src_p):
    """True (with a clear message) when the source DB hasn't been built yet.
    Keeps a fresh clone from dying on a raw DuckDB traceback."""
    if os.path.exists(src_p):
        return False
    log(f"  skip: {os.path.basename(src_p)} not found in {DB_DIR}. "
        "Build it with the matching ingest script first (see README).")
    return True


def compact_spectrum():
    """-> True when spectrum_c.duckdb is complete and safe to swap in."""
    src_p = os.path.join(DB_DIR, "spectrum.duckdb")
    if _missing(src_p):
        return False
    dst = duckdb.connect(os.path.join(DB_DIR, "spectrum_c.duckdb"))
    dst.execute("CREATE TABLE IF NOT EXISTS done (k VARCHAR)")
    dst.execute(f"ATTACH '{src_p}' AS s (READ_ONLY)")
    if not _skip(dst, "meta"):
        dst.execute("CREATE TABLE meta AS SELECT * FROM s.meta")
        dst.execute("INSERT INTO done VALUES ('meta')")
    for t in ("lvl_d1", "lvl_h6", "lvl_h1", "lvl_m10", "raw"):
        if _skip(dst, t):
            continue
        t0 = time.time()
        dst.execute(f"DROP TABLE IF EXISTS {t}")
        # dBm*10 as SMALLINT: the API already rounds to 0.1 dBm, so lossless
        # as served. ORDER BY improves zonemap pruning + bitpacking.
        dst.execute(f"""CREATE TABLE {t} AS
            SELECT sensor, freq, CAST(t AS BIGINT) AS t,
                   CAST(ROUND(mx*10) AS SMALLINT) AS mx,
                   CAST(ROUND(md*10) AS SMALLINT) AS md,
                   CAST(ROUND(mn*10) AS SMALLINT) AS mn
            FROM s.{t} ORDER BY sensor, t""")
        dst.execute("INSERT INTO done VALUES (?)", [t])
        log(f"  spectrum.{t} in {time.time()-t0:.0f}s")
    dst.close()
    log("spectrum_c.duckdb complete")
    return True


def _copy_meta(dst, src_path, table):
    dst.execute(f"ATTACH '{src_path}' AS msrc (READ_ONLY)")
    dst.execute(f"DROP TABLE IF EXISTS {table}")
    dst.execute(f"CREATE TABLE {table} AS SELECT * FROM msrc.{table}")
    dst.execute("DETACH msrc")
    dst.execute("INSERT INTO done VALUES ('meta')")


def compact_psd():
    """-> True only if every sensor round-tripped with the same row count."""
    src_p = os.path.join(DB_DIR, "psd.duckdb")
    if _missing(src_p):
        return False
    src = duckdb.connect(src_p, read_only=True)
    dst = duckdb.connect(os.path.join(DB_DIR, "psd_c.duckdb"))
    _prep(dst, """CREATE TABLE IF NOT EXISTS psd_chunk (
        sensor VARCHAR, t0 DOUBLE, t1 DOUBLE, n INT, times BLOB, specs BLOB)""")
    if not _skip(dst, "meta"):
        _copy_meta(dst, src_p, "psd_meta")
    sensors = [r[0] for r in src.execute("SELECT DISTINCT sensor FROM psd ORDER BY 1").fetchall()]
    bad = []
    for s in sensors:
        if _skip(dst, s):
            log(f"  psd {s}: already done")
            continue
        t0 = time.time()
        dst.execute("DELETE FROM psd_chunk WHERE sensor=?", [s])   # partial from a crash
        cur = src.execute("SELECT t, spec FROM psd WHERE sensor=? ORDER BY t", [s])
        total = 0
        dst.execute("BEGIN")
        while True:
            rows = cur.fetchmany(PSD_CHUNK)
            if not rows:
                break
            ts = np.array([r[0] for r in rows], dtype=np.float64)
            dst.execute("INSERT INTO psd_chunk VALUES (?,?,?,?,?,?)",
                        [s, float(ts[0]), float(ts[-1]), len(rows),
                         zlib.compress(ts.tobytes(), Z),
                         zlib.compress(b"".join(r[1] for r in rows), Z)])
            total += len(rows)
        dst.execute("COMMIT")
        want = src.execute("SELECT count(*) FROM psd WHERE sensor=?", [s]).fetchone()[0]
        if total != want:
            log(f"  psd {s}: MISMATCH {total} != {want}. NOT marking done")
            bad.append(s)
            continue
        dst.execute("INSERT INTO done VALUES (?)", [s])
        log(f"  psd {s}: {total} spectra in {time.time()-t0:.0f}s")
    src.close(); dst.close()
    if bad:
        log(f"psd_c.duckdb INCOMPLETE for {', '.join(bad)}; not swapping it in")
        return False
    log("psd_c.duckdb complete")
    return True


def compact_pfp():
    """-> True only if every sensor round-tripped with the same row count."""
    src_p = os.path.join(DB_DIR, "pfp.duckdb")
    if _missing(src_p):
        return False
    src = duckdb.connect(src_p, read_only=True)
    dst = duckdb.connect(os.path.join(DB_DIR, "pfp_c.duckdb"))
    _prep(dst, """CREATE TABLE IF NOT EXISTS pfp_chunk (
        sensor VARCHAR, freq DOUBLE, t0 DOUBLE, t1 DOUBLE, n INT, times BLOB, frames BLOB)""")
    if not _skip(dst, "meta"):
        _copy_meta(dst, src_p, "pfp_meta")
    sensors = [r[0] for r in src.execute("SELECT DISTINCT sensor FROM pfp ORDER BY 1").fetchall()]
    bad = []
    for s in sensors:
        if _skip(dst, s):
            log(f"  pfp {s}: already done")
            continue
        t0 = time.time()
        dst.execute("DELETE FROM pfp_chunk WHERE sensor=?", [s])
        cur = src.execute("SELECT freq, t, frame FROM pfp WHERE sensor=? ORDER BY freq, t", [s])
        cf, ts, fr, total = None, [], [], 0

        def flush():
            if not ts:
                return
            ta = np.array(ts, dtype=np.float64)
            dst.execute("INSERT INTO pfp_chunk VALUES (?,?,?,?,?,?,?)",
                        [s, cf, float(ta[0]), float(ta[-1]), len(ts),
                         zlib.compress(ta.tobytes(), Z),
                         zlib.compress(b"".join(fr), Z)])

        dst.execute("BEGIN")
        while True:
            rows = cur.fetchmany(16384)
            if not rows:
                break
            for f, t, frame in rows:
                if f != cf or len(ts) >= PFP_CHUNK:
                    flush()
                    cf, ts, fr = f, [], []
                ts.append(t); fr.append(frame)
                total += 1
        flush()
        dst.execute("COMMIT")
        want = src.execute("SELECT count(*) FROM pfp WHERE sensor=?", [s]).fetchone()[0]
        if total != want:
            log(f"  pfp {s}: MISMATCH {total} != {want}. NOT marking done")
            bad.append(s)
            continue
        dst.execute("INSERT INTO done VALUES (?)", [s])
        log(f"  pfp {s}: {total} frames in {time.time()-t0:.0f}s")
    src.close(); dst.close()
    if bad:
        log(f"pfp_c.duckdb INCOMPLETE for {', '.join(bad)}; not swapping it in")
        return False
    log("pfp_c.duckdb complete")
    return True


def swap_in(name):
    """Move <name>_c.duckdb onto <name>.duckdb, keeping a .bak of the original.

    Doing this here is the point: leaving the compacted file sitting next to
    the original under a different name is how a run ends up "finished" while
    the server still reads the old file.
    """
    built = os.path.join(DB_DIR, f"{name}_c.duckdb")
    live = os.path.join(DB_DIR, f"{name}.duckdb")
    if not os.path.exists(built):
        return False
    bak = live + ".bak"
    try:
        if os.path.exists(live):
            if os.path.exists(bak):
                os.remove(bak)
            os.replace(live, bak)
        os.replace(built, live)
    except OSError as e:
        log(f"  could not swap {name}_c.duckdb -> {name}.duckdb: {e}")
        log("    Stop serve.py (it holds the file open on Windows) and re-run, "
            "or rename the files by hand.")
        return False
    log(f"  {name}.duckdb <- {name}_c.duckdb "
        f"({os.path.getsize(live)/1e9:.2f} GB; previous kept as "
        f"{os.path.basename(bak)})")
    return True


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--no-swap", action="store_true",
                    help="leave the compacted files as *_c.duckdb instead of "
                         "moving them into place")
    args = ap.parse_args()

    log("compacting spectrum.duckdb ...")
    ok = {"spectrum": compact_spectrum()}
    log("compacting psd.duckdb ...")
    ok["psd"] = compact_psd()
    log("compacting pfp.duckdb (the big one) ...")
    ok["pfp"] = compact_pfp()
    for f in ("spectrum_c.duckdb", "psd_c.duckdb", "pfp_c.duckdb"):
        p = os.path.join(ROOT, f)      # the compacted DBs land beside serve.py
        if os.path.exists(p):
            log(f"  {f}: {os.path.getsize(p)/1e9:.2f} GB")
    if args.no_swap:
        log("--no-swap: rename *_c.duckdb yourself before serve.py will see them")
    else:
        log("swapping compacted files into place ...")
        # Only a verified-complete build file replaces a live database.
        swapped = [n for n in ("spectrum", "psd", "pfp") if ok[n] and swap_in(n)]
        if not swapped:
            log("  nothing to swap")
    log("DONE ALL")
