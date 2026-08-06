"""ingest_all_stats.py -- pull median and mean PSD for all 10 SEA sensors,
then build coarse levels and compact. One command instead of ~22.

    cd C:\\Users\\you\\ATLAS
    python ingest\\ingest_all_stats.py

Safe to re-run: psd_ingest.py skips days it already has, so a partial or
interrupted run just picks up where it left off. Max is untouched by any of
this -- only psd_median.duckdb and psd_mean.duckdb are written. A sensor
whose ingest fails (e.g. no CSVs on Box for it) is reported and skipped;
the rest still run, and the summary at the end lists exactly which ones.
"""
import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT_REPO = os.path.dirname(HERE)

# The 10 SEA sensors of the NASCTN CBRS deployment (NIST TN 2359).
# Pass --sensors to override or subset.
SENSORS = ["GMM", "Catalina-Directional", "Catalina-Omni", "Midway", "HU",
           "NIT", "CBBT-Directional", "CBBT-Omni", "PtLoma-Directional",
           "PtLoma-Omni"]
STATS = ["median", "mean"]
DEFAULT_BOX_ROOT = os.environ.get("SEA_DATA_ROOT") or r"C:\Users\you\Box\SEA-DATA"


def run(args, label):
    print(f"---- {label} ----", flush=True)
    p = subprocess.run([sys.executable] + args, cwd=ROOT_REPO)
    ok = p.returncode == 0
    if not ok:
        print(f"  !! {label} FAILED (exit {p.returncode}) -- continuing with the rest")
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=DEFAULT_BOX_ROOT,
                     help="Box SEA-DATA folder (default: %(default)s)")
    ap.add_argument("--sensors", nargs="*", default=None,
                     help="subset of sensors, default all 10")
    ap.add_argument("--stats", nargs="*", default=None, choices=STATS,
                     help="subset of stats, default median and mean")
    ap.add_argument("--skip-levels", action="store_true",
                     help="ingest only; skip build_psd_levels.py + compact_db.py")
    args = ap.parse_args()

    sensors = args.sensors or SENSORS
    stats = args.stats or STATS
    ingest_py = os.path.join(HERE, "psd_ingest.py")
    levels_py = os.path.join(HERE, "build_psd_levels.py")
    compact_py = os.path.join(HERE, "compact_db.py")

    print(f"Ingesting {stats} for {len(sensors)} sensor(s) from {args.root}\n")
    results = {}
    for sensor in sensors:
        for stat in stats:
            label = f"{sensor} : {stat}"
            results[label] = run(
                [ingest_py, sensor, "--stat", stat, "--root", args.root], label)
            print()

    if not args.skip_levels:
        # Compact first, then build the pyramid on the compacted file: levels
        # built over the row schema used to be thrown away by compaction, and
        # building them afterwards is cheaper anyway (fewer, larger reads).
        # compact_db.py now carries psd_lvl across either way, so this order is
        # belt and braces rather than the only thing standing between you and a
        # silently slow viewer.
        run([compact_py], "compact_db")
        for stat in stats:
            run([levels_py, "--stat", stat], f"build_psd_levels --stat {stat}")

    failed = [k for k, ok in results.items() if not ok]
    print("\n" + "=" * 66)
    if failed:
        print(f"{len(failed)} of {len(results)} sensor/stat combo(s) failed:")
        for f in failed:
            print(f"  - {f}")
        print("Re-run the same command; already-ingested days are skipped.")
    else:
        print(f"All {len(results)} sensor/stat combo(s) ingested cleanly.")
    print("Reload the viewer -- median/mean buttons light up in PSD mode for "
          "every sensor that now has data.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
