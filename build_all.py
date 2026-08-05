"""build_all.py: build + compact every sensor's databases IN PARALLEL.

Runs the same ATLAS scripts a by-hand build uses -- psd_ingest.py,
pfp_ingest.py, compact_db.py, build_db.py -- just per sensor and concurrently.
DuckDB allows one writer per file, so the sequential build was one sensor at a
time by necessity; giving each sensor its OWN database removes that constraint,
and serve.py reads a folder of per-sensor files as one dataset.

Each sensor builds in its own subfolder (so compact_db.py's fixed filenames
work untouched), then the compacted results are moved up as
psd_<sensor>.duckdb / pfp_<sensor>.duckdb. Summaries build once at the end:
one CSV, one file, nothing to parallelise.

    python build_all.py <csv-root> <out-dir> [workers]
"""
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.abspath(
    os.path.join(__file__, "..")))                    # unused fallback
ATLAS = os.environ.get("ATLAS_ROOT") or os.path.dirname(os.path.abspath(__file__))
ING = os.path.join(ATLAS, "ingest")

sys.path.insert(0, ING)
import psd_ingest                                     # noqa: E402  (discover)
import pfp_ingest                                     # noqa: E402


def run(argv, env, log):
    with open(log, "a") as fh:
        return subprocess.run(argv, env=env, cwd=ATLAS,
                              stdout=fh, stderr=subprocess.STDOUT).returncode


def build_sensor(sensor, psd_root, pfp_root, out, logdir):
    t0 = time.time()
    work = os.path.join(out, "_build", sensor)
    os.makedirs(work, exist_ok=True)
    log = os.path.join(logdir, f"{sensor}.log")
    env = {**os.environ, "ATLAS_DB_DIR": work}
    for var in ("SPECTRUM_DB", "PSD_DB", "PFP_DB", "IQ_DB"):
        env.pop(var, None)                 # classic names inside `work`
    steps = [
        ("psd", [sys.executable, os.path.join(ING, "psd_ingest.py"),
                 sensor, "--root", psd_root]),
        ("pfp", [sys.executable, os.path.join(ING, "pfp_ingest.py"),
                 sensor, "--root", pfp_root]),
        ("compact", [sys.executable, os.path.join(ING, "compact_db.py")]),
    ]
    for name, argv in steps:
        if run(argv, env, log) != 0:
            return sensor, None, f"{name} failed (see {log})"
    for kind in ("psd", "pfp"):
        src = os.path.join(work, f"{kind}.duckdb")
        if os.path.exists(src):
            os.replace(src, os.path.join(out, f"{kind}_{sensor}.duckdb"))
    shutil.rmtree(work, ignore_errors=True)
    return sensor, time.time() - t0, None


def subdir(root, *names):
    """The first existing subfolder of `root` among `names`, else root itself.

    Scoping matters when `root` is a streamed drive (Box Drive, OneDrive):
    build_db.py opens every CSV under its directory to check the header, so
    pointing it at the whole dataset would download hundreds of GB of PSD/PFP
    files just to discover they are not Summaries. Each stage gets only its
    own folder to look at.
    """
    for n in names:
        d = os.path.join(root, n)
        if os.path.isdir(d):
            return d
    return root


def main():
    csv_root, out = os.path.abspath(sys.argv[1]), os.path.abspath(sys.argv[2])
    workers = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    os.makedirs(out, exist_ok=True)
    logdir = os.path.join(out, "_logs")
    os.makedirs(logdir, exist_ok=True)

    psd_root = subdir(csv_root, "PSD", "spectra")
    pfp_root = subdir(csv_root, "PFP Aligned", "PFP", "frames")
    sum_root = subdir(csv_root, "Summaries", "summaries")
    sensors = sorted(set(psd_ingest.discover(psd_root))
                     | set(pfp_ingest.discover(pfp_root)))
    if not sensors:
        sys.exit(f"no PSD/PFP CSVs found under {csv_root}")
    print(f"{len(sensors)} sensor(s), {workers} at a time: "
          f"{', '.join(sensors)}\n")

    t0, bad = time.time(), []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for sensor, dt, err in ex.map(
                lambda s: build_sensor(s, psd_root, pfp_root, out, logdir), sensors):
            if err:
                bad.append(sensor)
                print(f"  FAIL {sensor}: {err}")
            else:
                print(f"  done {sensor}  ({dt/60:.1f} min)")

    # Summaries: one multi-sensor CSV -> one spectrum.duckdb, sequential.
    env = {**os.environ, "ATLAS_DB_DIR": out}
    for var in ("SPECTRUM_DB", "PSD_DB", "PFP_DB", "IQ_DB"):
        env.pop(var, None)
    rc = run([sys.executable, os.path.join(ING, "build_db.py"),
              "--csv-dir", sum_root], env, os.path.join(logdir, "summaries.log"))
    print("  done summaries -> spectrum.duckdb" if rc == 0 else
          "  (no Summaries CSV found; skipping the summary layer)")

    shutil.rmtree(os.path.join(out, "_build"), ignore_errors=True)
    total = sum(os.path.getsize(os.path.join(out, f))
                for f in os.listdir(out)
                if f.endswith(".duckdb"))
    print(f"\n{time.time()-t0:.0f}s total. {out}: "
          f"{total/1e6:.1f} MB of compacted databases.")
    print("serve it:  ATLAS_DB_DIR set to this folder, then python serve.py")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
