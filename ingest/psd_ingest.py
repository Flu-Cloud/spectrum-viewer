"""
psd_ingest.py: ingest PSD numbers into a compact, fast store.

Each capture's 2250-bin spectrum is quantized to int8 (0.35 dB steps over
[-180,-90] dBm/Hz) and stored as one BLOB row: (sensor, t, spec). That's ~1
row per capture (~180k) instead of 398M long-form rows, so ingest skips the
expensive UNPIVOT, the DB is ~4x smaller, and rendering is just a fetch + numpy
bin. Re-render at any frequency zoom stays crisp (the numbers are preserved).

NOTE: this writes the ROW-PER-CAPTURE schema (table `psd`). The live server
reads the compacted chunk schema (`psd_chunk`, built by compact_db.py). After
ingesting new data: run compact_db.py, then swap psd_c.duckdb -> psd.duckdb.

    cd C:\\Users\\pipyt\\spectrum-viewer
    py psd_ingest.py CBBT-Directional            # one sensor
    py psd_ingest.py CBBT-Directional --limit 5  # quick test
"""

import os
import sys
import time

import duckdb
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)   # repo root (scripts live in ingest/)
DATA_ROOT = os.environ.get("SEA_DATA_ROOT", r"C:\Users\pipyt\Box\SEA-DATA")
SUMM_DB = os.path.join(ROOT, "spectrum.duckdb")
PSD_DB = os.environ.get("PSD_DB", os.path.join(ROOT, "psd.duckdb"))

F0 = 3530040000.0    # first PSD bin (Hz)
DF = 80000.0         # bin spacing (Hz)
NF = 2250            # bins
QMIN, QMAX = -180.0, -90.0   # int8 quantization range (dBm/Hz)

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


def read_specs(path, rcon):
    """CSV -> (epoch_seconds[n], quantized uint8 [n,2250])."""
    res = rcon.execute("SELECT * FROM read_csv_auto(?, header=true)", [path])
    cols = [c[0] for c in res.description]
    d = res.fetchnumpy()
    ts = d[cols[0]].astype("datetime64[us]").astype("int64") / 1e6   # epoch sec
    P = np.stack([np.asarray(d[c], dtype=np.float64) for c in cols[1:]], axis=1)
    Q = np.clip(np.round((P - QMIN) / (QMAX - QMIN) * 255.0), 0, 255).astype(np.uint8)
    return ts, Q


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: py psd_ingest.py <sensor> [--limit N]")
    sensor = sys.argv[1]
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None

    days = days_for(sensor)
    if limit:
        days = days[:limit]
    print(f"{sensor}: {len(days)} days to ingest (compact int8 BLOB)")

    con = duckdb.connect(PSD_DB)
    con.execute("CREATE TABLE IF NOT EXISTS psd (sensor VARCHAR, t DOUBLE, spec BLOB)")
    con.execute("""CREATE TABLE IF NOT EXISTS psd_meta (
        sensor VARCHAR, f0 DOUBLE, df DOUBLE, nf INT, qmin DOUBLE, qmax DOUBLE,
        t_min DOUBLE, t_max DOUBLE, captures BIGINT)""")
    # resumable: skip days already ingested
    existing = set(str(r[0]) for r in con.execute(
        "SELECT DISTINCT CAST(to_timestamp(t) AS DATE) FROM psd WHERE sensor=?", [sensor]).fetchall())
    if existing:
        print(f"  resuming: {len(existing)} days already ingested, skipping those")
    rcon = duckdb.connect()   # separate handle for CSV reads

    t0 = time.time()
    done = miss = err = caps = 0
    for i, day in enumerate(days, 1):
        if day in existing:
            done += 1
            continue
        path = os.path.join(DATA_ROOT, "PSD", f"{day}_{sensor}_max.csv")
        if not os.path.exists(path):
            miss += 1
            continue
        try:
            ts, Q = read_specs(path, rcon)
            con.executemany(
                "INSERT INTO psd VALUES (?,?,?)",
                [(sensor, float(ts[j]), Q[j].tobytes()) for j in range(len(ts))])
            done += 1; caps += len(ts)
        except Exception as e:
            err += 1
            print(f"  ERR {os.path.basename(path)}: {e}")
        if i % 25 == 0 or i == len(days):
            rate = i / max(time.time() - t0, 1e-6)
            print(f"  [{i}/{len(days)}] caps={caps:,} done={done} miss={miss} err={err} {rate:.1f} days/s")

    rng = con.execute("SELECT min(t), max(t), count(*) FROM psd WHERE sensor=?", [sensor]).fetchone()
    con.execute("DELETE FROM psd_meta WHERE sensor=?", [sensor])
    con.execute("INSERT INTO psd_meta VALUES (?,?,?,?,?,?,?,?,?)",
                [sensor, F0, DF, NF, QMIN, QMAX, rng[0], rng[1], rng[2]])
    con.close()
    size = os.path.getsize(PSD_DB) / 1e9
    print(f"\nDone in {(time.time()-t0)/60:.1f} min. {sensor}: {caps:,} captures. "
          f"DB {PSD_DB} = {size:.2f} GB (all sensors)")


if __name__ == "__main__":
    main()
