"""fast_stat_ingest.py -- build psd_median.duckdb and psd_mean.duckdb FAST,
straight from the Box API, in parallel. Run from the ATLAS folder:

    python ingest\\fast_stat_ingest.py

Why this exists: ingest_all_stats.bat reads CSVs through Box Drive, whose
on-demand hydration serves one file at a time at ~50-100 KB/s -- a 30+ hour
job. This script downloads the same ~84 GB with 15 parallel API connections
(megabytes/sec each), converts several sensors concurrently, and lands the
finished databases next to psd.duckdb. Hours, not days.

Needs box_tokens.json next to this script (your own Box OAuth tokens -- keep
the file out of git; .gitignore covers it). Fully resumable: rerun after any
interruption and it continues where it stopped. psd.duckdb (max) is never
touched. Work happens in _stat_build/ (gitignored); the finished files are
moved into place only at the very end.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)                      # the ATLAS folder
WORK = os.path.join(ROOT, "_stat_build")
TOKENS = os.path.join(HERE, "box_tokens.json")
MANIFEST = os.path.join(HERE, "manifest_stats.json")
STATS = ("median", "mean")

_tok_lock = threading.Lock()


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---- Box API over stdlib urllib (no extra dependencies) --------------------

def _save_tokens(t):
    tmp = TOKENS + ".tmp"
    with open(tmp, "w") as f:
        json.dump(t, f)
    os.replace(tmp, TOKENS)


def get_token():
    """Access token, refreshed when <10 min left. Refresh tokens are
    single-use, so refresh under a lock and persist immediately."""
    with _tok_lock:
        t = json.load(open(TOKENS))
        if t.get("expires_at", 0) - time.time() > 600:
            return t["access_token"]
        data = urllib.parse.urlencode({
            "grant_type": "refresh_token", "refresh_token": t["refresh_token"],
            "client_id": t["client_id"], "client_secret": t["client_secret"],
        }).encode()
        req = urllib.request.Request("https://api.box.com/oauth2/token", data=data)
        with urllib.request.urlopen(req, timeout=30) as r:
            j = json.load(r)
        t.update(access_token=j["access_token"], refresh_token=j["refresh_token"],
                 expires_at=time.time() + j.get("expires_in", 3600))
        _save_tokens(t)
        log("refreshed the Box access token")
        return t["access_token"]


def download(fid, dest, size=None, tries=5):
    """GET a file's content, verify the size, retry with backoff."""
    for attempt in range(tries):
        try:
            req = urllib.request.Request(
                f"https://api.box.com/2.0/files/{fid}/content",
                headers={"Authorization": f"Bearer {get_token()}"})
            with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as f:
                shutil.copyfileobj(r, f, 1 << 20)
            if size and os.path.getsize(dest) != size:
                raise IOError(f"size mismatch {os.path.getsize(dest)} != {size}")
            return
        except Exception as e:                                   # noqa: BLE001
            if attempt == tries - 1:
                raise
            wait = 3 * (attempt + 1)
            if isinstance(e, urllib.error.HTTPError) and e.code == 429:
                wait = int(e.headers.get("Retry-After", wait) or wait)
            time.sleep(wait)


# ---- per-sensor conversion (download batch -> psd_ingest --stat -> tidy) ---

def done_days(row_db, sensor):
    if not os.path.exists(row_db):
        return set()
    try:
        import duckdb
        con = duckdb.connect(row_db, read_only=True)
        days = {str(r[0]) for r in con.execute(
            "SELECT DISTINCT CAST(to_timestamp(t) AT TIME ZONE 'UTC' AS DATE) "
            "FROM psd WHERE sensor=?", [sensor]).fetchall()}
        con.close()
        return days
    except Exception:
        return set()


def convert_sensor(sensor, man, batch, dl_threads):
    wdir = os.path.join(WORK, sensor)
    ddir = os.path.join(WORK, "csv", sensor)
    os.makedirs(wdir, exist_ok=True)
    os.makedirs(ddir, exist_ok=True)
    for stat in STATS:
        row_db = os.path.join(wdir, f"psd_{stat}.duckdb")
        chunk_db = os.path.join(wdir, f"psd_{stat}_c.duckdb")
        files = man[stat].get(sensor, {})
        if os.path.exists(chunk_db) and not os.path.exists(row_db):
            log(f"{sensor} {stat}: already converted"); continue
        todo = sorted(set(files) - done_days(row_db, sensor))
        log(f"{sensor} {stat}: {len(todo)} of {len(files)} days to go")
        for i in range(0, len(todo), batch):
            chunk = todo[i:i + batch]

            def dl(day):
                f = files[day]
                dest = os.path.join(ddir, f["name"])
                if not (os.path.exists(dest) and os.path.getsize(dest) == f["size"]):
                    download(f["id"], dest, f["size"])
            with ThreadPoolExecutor(dl_threads) as ex:
                list(ex.map(dl, chunk))
            env = {**os.environ, "PSD_DB": os.path.join(wdir, "psd.duckdb")}
            p = subprocess.run(
                [sys.executable, os.path.join(HERE, "psd_ingest.py"), sensor,
                 "--stat", stat, "--root", ddir],
                env=env, cwd=ROOT, capture_output=True, text=True)
            if p.returncode != 0:
                log(f"{sensor} {stat}: ingest error\n" + (p.stdout + p.stderr)[-400:])
            for n in os.listdir(ddir):
                try:
                    os.remove(os.path.join(ddir, n))
                except OSError:
                    pass
            log(f"{sensor} {stat}: {min(i + batch, len(todo))}/{len(todo)} days")
    # compact this sensor's row DBs to the chunk schema serve.py reads
    if any(os.path.exists(os.path.join(wdir, f"psd_{s}.duckdb")) for s in STATS):
        env = {**os.environ, "ATLAS_DB_DIR": wdir}
        p = subprocess.run([sys.executable, os.path.join(HERE, "compact_db.py"),
                            "--no-swap"], env=env, cwd=ROOT,
                           capture_output=True, text=True)
        if p.returncode != 0:
            log(f"{sensor}: compact error\n" + (p.stdout + p.stderr)[-400:])
            return False
        for s in STATS:
            row = os.path.join(wdir, f"psd_{s}.duckdb")
            if os.path.exists(os.path.join(wdir, f"psd_{s}_c.duckdb")) and os.path.exists(row):
                os.remove(row)
    log(f"{sensor}: DONE")
    return True


# ---- merge the per-sensor chunk databases into one file per stat -----------

CHUNK_DDL = ("sensor VARCHAR, t0 DOUBLE, t1 DOUBLE, n INT, times BLOB, specs BLOB")
META_DDL = ("sensor VARCHAR, f0 DOUBLE, df DOUBLE, nf INT, qmin DOUBLE, "
            "qmax DOUBLE, t_min DOUBLE, t_max DOUBLE, captures BIGINT")


def merge_stat(stat, sensors):
    import duckdb
    out = os.path.join(WORK, f"psd_{stat}.duckdb")
    if os.path.exists(out):
        os.remove(out)
    con = duckdb.connect(out)
    con.execute(f"CREATE TABLE psd_chunk ({CHUNK_DDL})")
    con.execute(f"CREATE TABLE psd_meta ({META_DDL})")
    merged = 0
    for s in sensors:
        part = os.path.join(WORK, s, f"psd_{stat}_c.duckdb")
        if not os.path.exists(part):
            log(f"merge {stat}: {s} missing, skipped"); continue
        con.execute(f"ATTACH '{part}' AS part (READ_ONLY)")
        con.execute("INSERT INTO psd_chunk SELECT * FROM part.psd_chunk")
        con.execute("INSERT INTO psd_meta SELECT * FROM part.psd_meta")
        con.execute("DETACH part")
        merged += 1
    rows, nsens, caps = con.execute(
        "SELECT count(*), count(DISTINCT sensor), sum(n) FROM psd_chunk").fetchone()
    con.execute("CHECKPOINT")
    con.close()
    log(f"psd_{stat}.duckdb: {nsens} sensors, {caps:,} captures, "
        f"{os.path.getsize(out)/1e9:.2f} GB")
    return merged


def build_levels(stat):
    env = {**os.environ, "PSD_DB": os.path.join(WORK, "psd.duckdb")}
    p = subprocess.run([sys.executable, os.path.join(HERE, "build_psd_levels.py"),
                        "--stat", stat], env=env, cwd=ROOT,
                       capture_output=True, text=True)
    tail = "\n".join((p.stdout + p.stderr).strip().splitlines()[-2:])
    log(f"levels {stat}: rc={p.returncode} | {tail}")


def place(stat):
    src = os.path.join(WORK, f"psd_{stat}.duckdb")
    dst = os.path.join(ROOT, f"psd_{stat}.duckdb")
    for leftover in (dst, dst + ".wal"):
        if os.path.exists(leftover):
            try:
                os.remove(leftover)
            except PermissionError:
                log(f"!! {os.path.basename(leftover)} is in use -- close the viewer/server "
                    f"(python serve.py) and rerun; the finished file is kept in _stat_build")
                return False
    os.replace(src, dst)
    log(f"placed {os.path.basename(dst)}")
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sensors", nargs="*", default=None,
                    help="subset of sensors (default: every sensor in the manifest)")
    ap.add_argument("--workers", type=int, default=5,
                    help="sensors converted concurrently (default 5)")
    ap.add_argument("--dl-threads", type=int, default=3,
                    help="parallel downloads per sensor (default 3)")
    ap.add_argument("--batch", type=int, default=15,
                    help="days per download/ingest batch (default 15)")
    args = ap.parse_args()

    if not os.path.exists(TOKENS):
        sys.exit(f"missing {TOKENS} -- the Box tokens file must sit next to this script")
    if not os.path.exists(MANIFEST):
        sys.exit(f"missing {MANIFEST}")
    man = json.load(open(MANIFEST))
    sensors = args.sensors or sorted(set(man["median"]) | set(man["mean"]))
    total_days = sum(len(man[s].get(x, {})) for s in STATS for x in sensors)
    log(f"{len(sensors)} sensors, {total_days:,} sensor-days across median+mean")

    t0 = time.time()
    with ThreadPoolExecutor(args.workers) as ex:
        results = list(ex.map(
            lambda s: convert_sensor(s, man, args.batch, args.dl_threads), sensors))
    if not all(results):
        sys.exit("some sensors failed to convert -- rerun to resume (nothing is lost)")

    for stat in STATS:
        merge_stat(stat, sensors)
        build_levels(stat)
    ok = all(place(stat) for stat in STATS)
    shutil.rmtree(os.path.join(WORK, "csv"), ignore_errors=True)
    log(f"{'ALL DONE' if ok else 'DONE (place step pending)'} in "
        f"{(time.time()-t0)/60:.0f} min. Reload the viewer -- the median/mean "
        f"buttons light up in PSD mode.")


if __name__ == "__main__":
    main()
