"""
build_db.py: Tier 1 ingest and time-pyramid builder for the RF spectrum viewer.

Reads every Summaries CSV under ingest/csv (searched recursively, so the layout
fetch.py produced works as-is), loads the columns we care about into a single
DuckDB database, then precomputes coarse "zoom levels" so the viewer never has
to scan raw rows when you're looking at months or years at a time.

This database powers the zoomed-out summary layer. It is NOT required by
psd_ingest.py or pfp_ingest.py; those read their own CSVs directly.

Run once (re-run any time the CSVs change):

    py ingest/build_db.py
    py ingest/build_db.py --csv-dir /path/to/summaries
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _require  # noqa: F401  -- deps message instead of a traceback

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cbrs_files                                    # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)   # repo root (scripts live in ingest/)
CSV_DIR = os.path.join(HERE, "csv")
DB_DIR = os.environ.get("ATLAS_DB_DIR") or ROOT
DB_PATH = os.environ.get("SPECTRUM_DB") or os.path.join(DB_DIR, "spectrum.duckdb")

# Summaries CSVs carry these columns; anything else in the folder is skipped
# with a reason rather than aborting the whole run.
NEEDED = ("sensor_name", "channel_frequency_mhz", "timestamp",
          "max", "median", "mean")

# Pyramid: bucket size in seconds -> table name. "raw" (native ~90s cadence) is
# kept as-is; these are the coarser pre-aggregated levels for zoomed-out views.
LEVELS = [
    ("lvl_m10", 600),      # 10 minutes
    ("lvl_h1", 3600),      # 1 hour
    ("lvl_h6", 21600),     # 6 hours
    ("lvl_d1", 86400),     # 1 day
]


def find_csvs(csv_dir):
    return cbrs_files.walk_ext(csv_dir, (".csv",))


def no_data(csv_dir):
    """Say what was searched for and how to get it, not just 'no CSV files'."""
    where = os.path.abspath(csv_dir)
    print(f"No CSV files found under {where}", file=sys.stderr)
    if not os.path.isdir(csv_dir):
        print("  That directory does not exist yet.", file=sys.stderr)
    print("\nThis script builds the zoomed-out summary layer from the CBRS\n"
          "Summaries CSVs. To download them into the right place:\n\n"
          "    python ingest/fetch.py <record-id> --filter Summaries \\\n"
          "        --dest ingest/csv --flat\n\n"
          "Or point at a copy you already have:\n\n"
          "    python ingest/build_db.py --csv-dir /path/to/summaries\n\n"
          "You do NOT need this database for the PSD or PFP layers; those read\n"
          "their own CSVs directly (python ingest/psd_ingest.py --list).",
          file=sys.stderr)
    return 1


def main():
    ap = argparse.ArgumentParser(
        description="Build spectrum.duckdb (the summary layer) from Summaries CSVs.")
    ap.add_argument("--csv-dir", default=CSV_DIR,
                    help=f"directory of Summaries CSVs (default {CSV_DIR})")
    args = ap.parse_args()

    files = find_csvs(args.csv_dir)
    if not files:
        return no_data(args.csv_dir)

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    # A WAL left behind by a previous run that crashed or was killed mid-build
    # survives the os.remove above (it's a separate file) and DuckDB replays it
    # automatically on connect -- against the brand-new, empty database this
    # run just created. That replay re-issues the old run's CREATE TABLE raw
    # and collides with the one below ("Table with name \"raw\" already
    # exists!"), leaving spectrum.duckdb unreadable. Deleting the stale WAL
    # alongside the database file is what actually starts clean.
    wal_path = DB_PATH + ".wal"
    if os.path.exists(wal_path):
        os.remove(wal_path)
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
    used = skipped = 0
    for i, f in enumerate(files, 1):
        name = os.path.basename(f)
        # read_csv_auto figures out the schema; we only pull the 6 columns we need.
        try:
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
        except duckdb.Error as e:
            # A PSD or PFP export sitting in the same folder is not a Summaries
            # CSV. Say so and keep going rather than aborting the whole build.
            skipped += 1
            first = str(e).strip().splitlines()[0]
            print(f"  [{i:2}/{len(files)}] {name:18}  skipped: {first}")
            continue
        used += 1
        n = con.execute("SELECT count(*) FROM raw").fetchone()[0]
        print(f"  [{i:2}/{len(files)}] {name:18}  total rows: {n:,}  ({time.time()-t0:.0f}s)")

    if used == 0:
        con.close()
        os.remove(DB_PATH)
        print(f"\nNone of the {len(files)} CSV file(s) under "
              f"{os.path.abspath(args.csv_dir)} are Summaries exports.\n"
              f"  A Summaries CSV has the columns: {', '.join(NEEDED)}.\n"
              "  PSD and PFP exports belong to psd_ingest.py / pfp_ingest.py "
              "instead.", file=sys.stderr)
        return 1
    if skipped:
        print(f"  ({skipped} file(s) skipped, not Summaries exports)")

    print("Building pyramid levels...")
    for tbl, bucket in LEVELS:
        con.execute(f"""
            CREATE TABLE {tbl} AS
            SELECT sensor, freq,
                   floor(t/{bucket})*{bucket} AS t,
                   max(mx) AS mx,      -- true peak of peaks
                   avg(md) AS md,      -- approx (mean of medians), fine for overview
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
    print("next: python serve.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
