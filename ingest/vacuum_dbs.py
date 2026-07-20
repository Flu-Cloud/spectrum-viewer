"""vacuum_dbs.py - rewrite the *_c.duckdb build files into densely-packed files.

The per-sensor transaction pattern in compact_db.py leaves ~25% of each file as
partially-filled blocks; COPY FROM DATABASE into a fresh file recovers it.
Also drops the resume-tracking `done` table. Replaces each *_c.duckdb in place.
"""
import os
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)   # repo root (scripts live in ingest/)

for name in ("spectrum_c", "psd_c", "pfp_c"):
    src = os.path.join(ROOT, f"{name}.duckdb")
    tmp = os.path.join(ROOT, f"{name}_v.duckdb")
    if not os.path.exists(src):
        continue
    if os.path.exists(tmp):
        os.remove(tmp)
    t0 = time.time()
    con = duckdb.connect(src)
    con.execute("DROP TABLE IF EXISTS done")
    con.close()
    con = duckdb.connect(tmp)
    con.execute(f"ATTACH '{src}' AS s (READ_ONLY)")
    con.execute(f"COPY FROM DATABASE s TO {name}_v")
    con.close()
    before, after = os.path.getsize(src) / 1e9, os.path.getsize(tmp) / 1e9
    os.replace(tmp, src)
    print(f"{name}: {before:.2f} -> {after:.2f} GB in {time.time()-t0:.0f}s", flush=True)
print("VACUUM DONE", flush=True)
