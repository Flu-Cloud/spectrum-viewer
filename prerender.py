"""
prerender.py  -  download + render PSD day-files from Box into local tiles.

Driven off the local summaries DB (spectrum.duckdb) so it NEVER enumerates the
huge online-only Box folder. For each (sensor, day) it builds the exact file
path, pulls that one CSV from Box, renders a small WebP tile, and moves on.

Parallel (several Box downloads at once), resumable (skips finished tiles), and
prints live per-file progress with an ETA.

    cd C:\\Users\\pipyt\\spectrum-viewer
    py prerender.py              # all sensors/days, stat=max
    py prerender.py --limit 20   # just the first 20 (quick proof)
    py prerender.py --workers 8  # more concurrent Box downloads
"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb

import tier2

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.environ.get("SEA_DATA_ROOT", r"C:\Users\pipyt\Box\SEA-DATA")
DB_PATH = os.path.join(HERE, "spectrum.duckdb")
STAT = "max"


def jobs_from_db():
    con = duckdb.connect(DB_PATH, read_only=True)
    rows = con.execute(
        "SELECT sensor, CAST(to_timestamp(t) AS DATE) d FROM raw GROUP BY 1,2 ORDER BY 1,2"
    ).fetchall()
    con.close()
    return [(s, str(d)) for s, d in rows]


def render_one(sensor, date):
    """Return (status, kb, seconds). status in {done, skip, missing, err}."""
    png_p, json_p = tier2.psd_tile(sensor, date, STAT)
    if os.path.exists(png_p) and os.path.exists(json_p):
        return ("skip", 0, 0.0)
    path = os.path.join(DATA_ROOT, "PSD", f"{date}_{sensor}_{STAT}.csv")
    if not os.path.exists(path):
        return ("missing", 0, 0.0)
    t = time.time()
    png, meta = tier2.render_psd_file(path)
    tier2.save_psd_tile(sensor, date, STAT, png, meta)
    return ("done", len(png) // 1024, time.time() - t)


def main():
    args = sys.argv[1:]
    limit = None
    workers = 6
    if "--limit" in args:
        limit = int(args[args.index("--limit") + 1])
    if "--workers" in args:
        workers = int(args[args.index("--workers") + 1])

    print(f"DATA_ROOT = {DATA_ROOT}")
    jobs = jobs_from_db()
    if limit:
        jobs = jobs[:limit]
    print(f"{len(jobs)} (sensor, day) tiles to consider · {workers} parallel downloads")

    lock = threading.Lock()
    counts = {"done": 0, "skip": 0, "missing": 0, "err": 0}
    t0 = time.time()
    n = len(jobs)

    def work(job):
        s, d = job
        try:
            status, kb, secs = render_one(s, d)
        except Exception as e:
            return job, ("err", 0, 0.0, str(e))
        return job, (status, kb, secs, None)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(work, j): j for j in jobs}
        i = 0
        for fut in as_completed(futs):
            (s, d), (status, kb, secs, errmsg) = fut.result()
            i += 1
            with lock:
                counts[status] = counts.get(status, 0) + 1
                done = counts["done"]
            if status == "done":
                elapsed = time.time() - t0
                eta = (n - i) / max(i / elapsed, 1e-6)
                print(f"  [{i}/{n}] {d}_{s}  {kb}KB  {secs:.1f}s  "
                      f"done={done} skip={counts['skip']} miss={counts['missing']} "
                      f"ETA {eta/60:.0f}m")
            elif status == "err":
                print(f"  [{i}/{n}] ERR {d}_{s}: {errmsg}")
            elif i % 200 == 0:
                print(f"  [{i}/{n}] ... skip={counts['skip']} miss={counts['missing']}")

    total = sum(os.path.getsize(os.path.join(dp, f))
                for dp, _, fs in os.walk(tier2.TILE_ROOT) for f in fs) if os.path.isdir(tier2.TILE_ROOT) else 0
    print(f"\nDone in {(time.time()-t0)/60:.1f} min. "
          f"rendered={counts['done']} skipped={counts['skip']} "
          f"missing={counts['missing']} errors={counts['err']}")
    print(f"Tiles: {total/1e9:.2f} GB at {tier2.TILE_ROOT}")


if __name__ == "__main__":
    main()
