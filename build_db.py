"""
build_db.py  -  Tier 1 ingest + time-pyramid builder for the RF spectrum viewer.

Reads every Summaries CSV in ./csv, loads the columns we care about into a single
DuckDB database, then precomputes coarse "zoom levels" so the viewer never has to
scan raw rows when you're looking at months or years at a time.

Run once (re-run any time the CSVs change):

    cd C:\\Users\\pipyt\\spectrum-viewer
    py build_db.py
"""

import glob
import os
import sys
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(HERE, "csv")
DB_PATH = os.path.join(HERE, "spectrum.duckdb")

# Pyramid: bucket size in seconds -> table name. "raw" (native ~90s cadence) is
# kept as-is; these are the coarser pre-aggregated levels for zoomed-out views.
LEVELS = [
    ("lvl_m10", 600),      # 10 minutes
    ("lvl_h1", 3600),      # 1 hour
    ("lvl_h6", 21600),     # 6 hours
    ("lvl_d1", 86400),     # 1 day
]


def main():
    files = sorted(glob.glob(os.path.join(CSV_DIR, "*.csv")))
    if not files:
        sys.exit(f"No CSV files found in {CSV_DIR}")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    con = duckdb.connect(DB_PATH)
    # Be polite with RAM on a laptop; DuckDB will spill to disk if needed.
    con.execute("PRAGMA memory_limit='3GB'")
    con.execute("PRAGMA threads=4")

    con.execute("""
        CREATE TABLE raw (
            sensor VARCHAR,   -- sensor_name
            freq   DOUBLE,    -- channel_frequency_mhz
            t      DOUBLE,    -- epoch seconds (UTC)
            mx     DOUBLE,    -- max power (dBm)
            md     DOUBLE,    -- median power (dBm)
            mn     DOUBLE     -- mean power (dBm)
        )
    """)

    print(f"Ingesting {len(files)} CSV files...")
    t0 = time.time()
    for i, f in enumerate(files, 1):
        name = os.path.basename(f)
        # read_csv_auto figures out the schema; we only pull the 6 columns we need.
        con.execute("""
            INSERT INTO raw
            SELECT
                sensor_name,
                channel_frequency_mhz,
                epoch(CAST(timestamp AS TIMESTAMPTZ)),
                "max", median, mean
            FROM read_csv_auto(?, header=true,
                               types={'channel_frequency_mhz':'DOUBLE',
                                      'max':'DOUBLE','median':'DOUBLE','mean':'DOUBLE'})
        """, [f])
        n = con.execute("SELECT count(*) FROM raw").fetchone()[0]
        print(f"  [{i:2}/{len(files)}] {name:18}  total rows: {n:,}  ({time.time()-t0:.0f}s)")

    print("Building pyramid levels...")
    for tbl, bucket in LEVELS:
        con.execute(f"""
            CREATE TABLE {tbl} AS
            SELECT sensor, freq,
                   floor(t/{bucket})*{bucket} AS t,
                   max(mx) AS mx,      -- true peak of peaks
                   avg(md) AS md,      -- approx (mean of medians) - fine for overview
                   avg(mn) AS mn,
                   count(*) AS c
            FROM raw
            GROUP BY sensor, freq, floor(t/{bucket})*{bucket}
            ORDER BY sensor, t
        """)
        rows = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl:10} bucket={bucket:>6}s  rows: {rows:,}")

    # Keep raw sorted for fast range scans at full zoom.
    con.execute("CREATE TABLE raw_sorted AS SELECT * FROM raw ORDER BY sensor, t")
    con.execute("DROP TABLE raw")
    con.execute("ALTER TABLE raw_sorted RENAME TO raw")

    # Metadata the viewer needs on load.
    con.execute("""
        CREATE TABLE meta AS
        SELECT sensor,
               min(t) AS t_min, max(t) AS t_max,
               count(*) AS n
        FROM raw GROUP BY sensor ORDER BY sensor
    """)

    print("\nSensors:")
    for sensor, tmin, tmax, n in con.execute(
            "SELECT sensor, t_min, t_max, n FROM meta").fetchall():
        days = (tmax - tmin) / 86400
        print(f"  {sensor:22} {n:>12,} rows  spanning {days:6.1f} days")

    freqs = [r[0] for r in con.execute(
        "SELECT DISTINCT freq FROM raw ORDER BY freq").fetchall()]
    print(f"\nFrequencies (MHz): {freqs}")

    con.close()
    size_gb = os.path.getsize(DB_PATH) / 1e9
    print(f"\nDone in {time.time()-t0:.0f}s. Database: {DB_PATH} ({size_gb:.2f} GB)")


if __name__ == "__main__":
    main()
