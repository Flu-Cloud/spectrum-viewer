"""
master_ingest.py  -  download + ingest ALL sensors (PSD + PFP) into build DBs,
then atomically swap them live and restart the server. Fully autonomous.

- Writes to psd_build.duckdb / pfp_build.duckdb so the LIVE layers keep serving
  the current data the whole time.
- Each per-sensor ingest is resumable (skips days already done), so this whole
  run survives interruption: just launch it again and it continues.
- When every sensor is done, it stops the server on :8090, moves the build DBs
  over the live ones, and restarts the server -> all sensors live.

    cd C:\\Users\\pipyt\\spectrum-viewer
    py master_ingest.py
"""

import os
import shutil
import subprocess
import sys
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
SUMM = os.path.join(HERE, "spectrum.duckdb")
BOX = os.environ.get("SEA_DATA_ROOT", r"C:\Users\pipyt\Box\SEA-DATA")
LIVE_PSD = os.path.join(HERE, "psd.duckdb")
LIVE_PFP = os.path.join(HERE, "pfp.duckdb")
PSD_BUILD = os.path.join(HERE, "psd_build.duckdb")
PFP_BUILD = os.path.join(HERE, "pfp_build.duckdb")

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def run(script, sensor, dbenv, dbpath):
    env = {**os.environ, dbenv: dbpath, "SEA_DATA_ROOT": BOX}
    log(f"========== {script} {sensor} ==========")
    subprocess.run([sys.executable, script, sensor], env=env, cwd=HERE)


def kill_server():
    out = subprocess.run(["netstat", "-ano"], capture_output=True, text=True).stdout
    for line in out.splitlines():
        if ":8090" in line and "LISTENING" in line.upper():
            pid = line.split()[-1]
            subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
            log(f"stopped server pid {pid}")


def main():
    con = duckdb.connect(SUMM, read_only=True)
    sensors = [r[0] for r in con.execute("SELECT sensor FROM meta ORDER BY sensor").fetchall()]
    con.close()
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
        [sys.executable, "serve.py"], env=env, cwd=HERE,
        stdout=open(os.path.join(HERE, "serve_out.log"), "a"),
        stderr=open(os.path.join(HERE, "serve_err.log"), "a"),
        creationflags=getattr(subprocess, "DETACHED_PROCESS", 0))
    log("server restarted with all sensors live. DONE.")


if __name__ == "__main__":
    main()
