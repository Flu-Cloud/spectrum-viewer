"""
pfp_ingest.py: ingest PFP (periodic-frame-power) numbers into a compact store.

PFP = power across a 10 ms frame (560 positions, ~17.86 us each) per 10 MHz
channel, one trace per ~4-min capture. We store one int8-quantized BLOB per
(channel, capture): (sensor, freq, t, frame). Re-render any channel/time window
crisply, like the PSD layer.

NOTE: this writes the ROW-PER-CAPTURE schema (table `pfp`). The live server
reads the compacted chunk schema (`pfp_chunk`, built by compact_db.py). After
ingesting new data: run compact_db.py, then swap pfp_c.duckdb -> pfp.duckdb.

    cd /path/to/spectrum-viewer
    py pfp_ingest.py CBBT-Directional               # default stat max_peak
    py pfp_ingest.py CBBT-Directional --limit 30    # quick prototype
    py pfp_ingest.py CBBT-Directional --stat mean_rms
"""

import os
import sys
import time

import duckdb
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)   # repo root (scripts live in ingest/)
# Where the CBRS source CSVs live. Set SEA_DATA_ROOT to your copy; defaults to
# ./SEA-DATA in the repo so a fresh clone never points at someone else's disk.
DATA_ROOT = os.environ.get("SEA_DATA_ROOT", os.path.join(ROOT, "SEA-DATA"))
SUMM_DB = os.path.join(ROOT, "spectrum.duckdb")
PFP_DB = os.environ.get("PFP_DB", os.path.join(ROOT, "pfp.duckdb"))

NPOS = 560
FRAME_MS = 10.0
QMIN, QMAX = -130.0, -10.0   # int8 range (dBm), ~0.47 dB/step

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def days_for(sensor):
    con = duckdb.connect(SUMM_DB, read_only=True)
    rows = con.execute(
        "SELECT CAST(to_timestamp(t) AS DATE) d FROM raw WHERE sensor=? GROUP BY 1 ORDER BY 1",
        [sensor]).fetchall()
    con.close()
    return [str(r[0]) for r in rows]


def read_frames(path, rcon):
    """CSV -> (epoch_seconds[n], freq_hz[n], quantized uint8 [n,560])."""
    res = rcon.execute("SELECT * FROM read_csv_auto(?, header=true)", [path])
    cols = [c[0] for c in res.description]              # datetime, frequency, 0..559
    d = res.fetchnumpy()
    ts = d[cols[0]].astype("datetime64[us]").astype("int64") / 1e6
    freq = np.asarray(d[cols[1]], dtype=np.float64)
    pos_cols = cols[2:]
    P = np.stack([np.asarray(d[c], dtype=np.float64) for c in pos_cols], axis=1)   # (n,560)
    Q = np.clip(np.round((P - QMIN) / (QMAX - QMIN) * 255.0), 0, 255).astype(np.uint8)
    return ts, freq, Q


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: py pfp_ingest.py <sensor> [--stat S] [--limit N]")
    sensor = sys.argv[1]
    stat = sys.argv[sys.argv.index("--stat") + 1] if "--stat" in sys.argv else "max_peak"
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None

    days = days_for(sensor)
    if limit:
        days = days[:limit]
    print(f"{sensor} PFP ({stat}): {len(days)} days")

    con = duckdb.connect(PFP_DB)
    con.execute("CREATE TABLE IF NOT EXISTS pfp (sensor VARCHAR, freq DOUBLE, t DOUBLE, frame BLOB)")
    con.execute("""CREATE TABLE IF NOT EXISTS pfp_meta (
        sensor VARCHAR, stat VARCHAR, npos INT, frame_ms DOUBLE, qmin DOUBLE, qmax DOUBLE,
        t_min DOUBLE, t_max DOUBLE, rows BIGINT)""")
    # resumable: skip days already ingested (so a long Box pull can restart)
    existing = set(str(r[0]) for r in con.execute(
        "SELECT DISTINCT CAST(to_timestamp(t) AS DATE) FROM pfp WHERE sensor=?", [sensor]).fetchall())
    if existing:
        print(f"  resuming: {len(existing)} days already ingested, skipping those")
    rcon = duckdb.connect()

    t0 = time.time()
    done = miss = err = nrows = 0
    for i, day in enumerate(days, 1):
        if day in existing:
            done += 1
            continue
        path = os.path.join(DATA_ROOT, "PFP Aligned", f"PFP_{day}_{sensor}_{stat}.csv")
        if not os.path.exists(path):
            miss += 1
            continue
        try:
            ts, freq, Q = read_frames(path, rcon)
            con.executemany(
                "INSERT INTO pfp VALUES (?,?,?,?)",
                [(sensor, float(freq[j]), float(ts[j]), Q[j].tobytes()) for j in range(len(ts))])
            done += 1; nrows += len(ts)
        except Exception as e:
            err += 1
            print(f"  ERR {os.path.basename(path)}: {e}")
        if i % 10 == 0 or i == len(days):
            print(f"  [{i}/{len(days)}] rows={nrows:,} done={done} miss={miss} err={err} "
                  f"{i/max(time.time()-t0,1e-6):.2f} days/s")

    r = con.execute("SELECT min(t), max(t), count(*) FROM pfp WHERE sensor=?", [sensor]).fetchone()
    con.execute("DELETE FROM pfp_meta WHERE sensor=?", [sensor])
    con.execute("INSERT INTO pfp_meta VALUES (?,?,?,?,?,?,?,?,?)",
                [sensor, stat, NPOS, FRAME_MS, QMIN, QMAX, r[0], r[1], r[2]])
    con.close()
    print(f"\nDone in {(time.time()-t0)/60:.1f} min. {nrows:,} frames. "
          f"DB = {os.path.getsize(PFP_DB)/1e9:.2f} GB")


if __name__ == "__main__":
    main()
