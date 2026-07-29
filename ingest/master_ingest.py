"""
master_ingest.py: download and ingest ALL sensors (PSD + PFP) into build DBs,
then atomically swap them live and restart the server. Fully autonomous.

- Writes to psd_build.duckdb / pfp_build.duckdb so the LIVE layers keep serving
  the current data the whole time.
- Each per-sensor ingest is resumable (skips days already done), so this whole
  run survives interruption: just launch it again and it continues.
- When every sensor is done, it moves the build DBs over the live ones and
  restarts the server -> all sensors live.

The swap installs the row-per-capture schema, which serve.py reads directly.
Run compact_db.py afterwards if you want the smaller files; it is optional.

Stopping a running server automatically only works on Windows. Elsewhere, stop
serve.py yourself before this reaches the swap (it will tell you).

    py ingest/master_ingest.py
"""

import os
import shutil
import subprocess
import sys
import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import psd_ingest                                            # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))       # ingest/ (holds the scripts)
ROOT = os.path.dirname(HERE)                             # repo root (holds serve.py + DBs)
SUMM = os.path.join(ROOT, "spectrum.duckdb")
BOX = os.environ.get("SEA_DATA_ROOT", os.path.join(ROOT, "SEA-DATA"))
LIVE_PSD = os.path.join(ROOT, "psd.duckdb")
LIVE_PFP = os.path.join(ROOT, "pfp.duckdb")
PSD_BUILD = os.path.join(ROOT, "psd_build.duckdb")
PFP_BUILD = os.path.join(ROOT, "pfp_build.duckdb")

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def run(script, sensor, dbenv, dbpath):
    env = {**os.environ, dbenv: dbpath, "SEA_DATA_ROOT": BOX}
    log(f"========== {script} {sensor} ==========")
    subprocess.run([sys.executable, os.path.join(HERE, script), sensor], env=env, cwd=ROOT)


def kill_server():
    """Stop a serve.py holding the live DBs open. Windows only; elsewhere the
    files can be replaced while open, so there is nothing to do."""
    if os.name != "nt":
        log("not Windows: if serve.py is running, restart it after the swap")
        return
    out = subprocess.run(["netstat", "-ano"], capture_output=True, text=True).stdout
    for line in out.splitlines():
        if ":8090" in line and "LISTENING" in line.upper():
            pid = line.split()[-1]
            subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
            log(f"stopped server pid {pid}")


def sensors_available():
    """Sensor list from the summary DB when it exists, else from the CSVs."""
    if os.path.exists(SUMM):
        con = duckdb.connect(SUMM, read_only=True)
        names = [r[0] for r in con.execute(
            "SELECT sensor FROM meta ORDER BY sensor").fetchall()]
        con.close()
        if names:
            return names
    return sorted(psd_ingest.discover(BOX, "max"))


def main():
    sensors = sensors_available()
    if not sensors:
        sys.exit(f"no sensors found. Looked in {SUMM} and for PSD CSVs under "
                 f"{os.path.abspath(BOX)}.\n"
                 "  Set SEA_DATA_ROOT to your copy of the data, then re-run.")
    log(f"sensors ({len(sensors)}): {sensors}")

    # seed the PSD build DB from the live one (CBBT-Directional already ingested)
    if not os.path.exists(PSD_BUILD) and os.path.exists(LIVE_PSD):
        shutil.copy(LIVE_PSD, PSD_BUILD)
        log(f"seeded {os.path.basename(PSD_BUILD)} from live psd.duckdb")

    t0 = time.time()
    for s in sensors:
        run("psd_ingest.py", s, "PSD_DB", PSD_BUILD)
        run("pfp_ingest.py", s, "PFP_DB", PFP_BUILD)
        log(f">>> {s} COMPLETE (elapsed {(time.time()-t0)/3600:.1f} hr)")

    log(f"ALL INGEST DONE in {(time.time()-t0)/3600:.1f} hr. Swapping build -> live ...")
    kill_server()
    time.sleep(3)
    for build, live in [(PSD_BUILD, LIVE_PSD), (PFP_BUILD, LIVE_PFP)]:
        if os.path.exists(build):
            try:
                os.replace(build, live)
                log(f"swapped {os.path.basename(build)} -> live")
            except Exception as e:
                log(f"swap FAILED for {build}: {e}")
    env = {**os.environ, "SEA_PORT": "8090"}
    subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "serve.py")], env=env, cwd=ROOT,
        stdout=open(os.path.join(ROOT, "serve_out.log"), "a"),
        stderr=open(os.path.join(ROOT, "serve_err.log"), "a"),
        creationflags=getattr(subprocess, "DETACHED_PROCESS", 0))
    log("server restarted with all sensors live. DONE.")


if __name__ == "__main__":
    main()
