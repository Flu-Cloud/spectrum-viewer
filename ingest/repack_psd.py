"""
repack_psd.py: convert the existing long-form psd.duckdb (sensor,t,fi,p) into
the compact int8-BLOB format (one spectrum per capture), locally. No Box needed.

    py repack_psd.py CBBT-Directional
"""
import os
import sys
import time

import duckdb
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)   # repo root (scripts live in ingest/)
SRC = os.path.join(ROOT, "psd.duckdb")
DST = os.path.join(ROOT, "psd_compact.duckdb")
F0, DF, NF = 3530040000.0, 80000.0, 2250
QMIN, QMAX = -180.0, -90.0
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def main():
    sensor = sys.argv[1] if len(sys.argv) > 1 else "CBBT-Directional"
    src = duckdb.connect(SRC, read_only=True)
    dst = duckdb.connect(DST)
    dst.execute("CREATE TABLE IF NOT EXISTS psd (sensor VARCHAR, t DOUBLE, spec BLOB)")
    dst.execute("""CREATE TABLE IF NOT EXISTS psd_meta (
        sensor VARCHAR, f0 DOUBLE, df DOUBLE, nf INT, qmin DOUBLE, qmax DOUBLE,
        t_min DOUBLE, t_max DOUBLE, captures BIGINT)""")
    dst.execute("DELETE FROM psd WHERE sensor=?", [sensor])
    dst.execute("DELETE FROM psd_meta WHERE sensor=?", [sensor])

    tmin, tmax, ncap = src.execute(
        "SELECT min(t), max(t), count(DISTINCT t) FROM psd WHERE sensor=?", [sensor]).fetchone()
    print(f"{sensor}: {ncap:,} captures, repacking day by day...")

    t0 = time.time()
    step = 86400.0
    t = tmin
    done = 0
    while t <= tmax:
        rows = src.execute(
            "SELECT t, list(p ORDER BY fi) AS spec FROM psd "
            "WHERE sensor=? AND t>=? AND t<? GROUP BY t ORDER BY t",
            [sensor, t, t + step]).fetchall()
        batch = []
        for tt, spec in rows:
            P = np.asarray(spec, dtype=np.float64)
            Q = np.clip(np.round((P - QMIN) / (QMAX - QMIN) * 255.0), 0, 255).astype(np.uint8)
            batch.append((sensor, float(tt), Q.tobytes()))
        if batch:
            dst.executemany("INSERT INTO psd VALUES (?,?,?)", batch)
            done += len(batch)
        t += step
        if done and (done // 5000) != ((done - len(batch)) // 5000):
            print(f"  {done:,}/{ncap:,} captures  ({done/max(time.time()-t0,1e-6):.0f}/s)")

    r = dst.execute("SELECT min(t), max(t), count(*) FROM psd WHERE sensor=?", [sensor]).fetchone()
    dst.execute("INSERT INTO psd_meta VALUES (?,?,?,?,?,?,?,?,?)",
                [sensor, F0, DF, NF, QMIN, QMAX, r[0], r[1], r[2]])
    src.close(); dst.close()
    print(f"\nDone in {(time.time()-t0)/60:.1f} min. {done:,} captures. "
          f"compact DB = {os.path.getsize(DST)/1e9:.2f} GB "
          f"(long-form was {os.path.getsize(SRC)/1e9:.2f} GB)")


if __name__ == "__main__":
    main()
