"""test_ingest.py: prove the CBRS "build your own database" path works.

Generates small synthetic Summaries / PSD / PFP CSVs, puts them in a folder
layout nobody would guess (nested, not the names the scripts used to hardcode),
and runs the real ingest scripts against them. Then it drives serve.py in a
fresh process to confirm the viewer renders the result, runs compact_db.py, and
confirms it still renders after the compacted files are swapped in.

Nothing here touches the databases beside serve.py: everything happens in a
temporary directory via the SPECTRUM_DB / PSD_DB / PFP_DB / ATLAS_DB_DIR
overrides.

    python examples/test_ingest.py          # exits 0 = PASS
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ING = os.path.join(ROOT, "ingest")
sys.path.insert(0, ING)

NF = 2250          # PSD bins, must match psd_ingest.NF
NPOS = 560         # PFP frame positions, must match pfp_ingest.NPOS
SENSOR = "TEST-Directional"
DAYS = ["2024-01-01", "2024-01-02"]

failed = 0


def check(name, ok, detail=""):
    global failed
    if not ok:
        failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{(' - ' + detail) if detail else ''}")
    return ok


def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        f.write(",".join(header) + "\n")
        for r in rows:
            f.write(",".join(str(v) for v in r) + "\n")


def make_data(root):
    """Synthetic CBRS exports, deliberately buried in an odd folder layout."""
    import numpy as np
    rng = np.random.default_rng(7)

    # PSD: <day>_<sensor>_max.csv , 6 captures of 2250 bins
    psd_dir = os.path.join(root, "mds2-test", "sea", "spectra", "level-1")
    for day in DAYS:
        rows = []
        for k in range(6):
            spec = np.round(-140 + 20 * rng.random(NF), 2)
            rows.append([f"{day} {k:02d}:00:00", *spec])
        write_csv(os.path.join(psd_dir, f"{day}_{SENSOR}_max.csv"),
                  ["datetime"] + [f"b{i}" for i in range(NF)], rows)

    # PFP: PFP_<day>_<sensor>_max_peak.csv , 4 captures x 560 positions.
    # The `frequency` column is in HERTZ, as the real NASCTN exports write it
    # and as pfp_ingest.py stores it verbatim -- the Summaries column next to it
    # is the one in MHz. This fixture used to say 3555.0, so pfp.duckdb held a
    # channel centre of 3555 Hz and every test exercised a unit the real data
    # never uses.
    pfp_dir = os.path.join(root, "mds2-test", "sea", "frames")
    for day in DAYS:
        rows = []
        for k in range(4):
            frame = np.round(-90 + 30 * rng.random(NPOS), 2)
            rows.append([f"{day} {k:02d}:30:00", 3555.0e6, *frame])
        write_csv(os.path.join(pfp_dir, f"PFP_{day}_{SENSOR}_max_peak.csv"),
                  ["datetime", "frequency"] + [f"p{i}" for i in range(NPOS)], rows)

    # Summaries, for build_db.py
    sm_dir = os.path.join(root, "mds2-test", "summaries")
    rows = []
    for day in DAYS:
        for hour in range(12):
            for f_mhz in (3555.0, 3565.0, 3575.0):
                rows.append([SENSOR, f_mhz, f"{day} {hour:02d}:00:00+00",
                             round(-60 - rng.random() * 5, 2),
                             round(-70 - rng.random() * 5, 2),
                             round(-75 - rng.random() * 5, 2)])
    write_csv(os.path.join(sm_dir, "Summaries_test.csv"),
              ["sensor_name", "channel_frequency_mhz", "timestamp",
               "max", "median", "mean"], rows)
    return psd_dir, pfp_dir, sm_dir


PROBE = r'''
import json, os, sys
sys.path.insert(0, os.environ["ATLAS_ROOT"])
import serve
c = serve.app.test_client()
out = {}
s = os.environ["SENSOR"]
m = c.get(f"/api/psd_meta?sensor={s}").get_json()
out["psd_meta"] = m
if m.get("has"):
    r = c.get(f"/api/psd_layer?sensor={s}&t0={m['t_min']-1}&t1={m['t_max']+1}&w=400&h=200")
    out["psd_layer"] = [r.status_code, r.mimetype, len(r.data)]
pm = c.get(f"/api/pfp_meta?sensor={s}").get_json()
out["pfp_meta"] = pm
if pm.get("has") and pm.get("freqs"):
    r = c.get(f"/api/pfp_frame?sensor={s}&freq={pm['freqs'][0]}"
              f"&t0={pm['t_min']-1}&t1={pm['t_max']+1}&h=200")
    out["pfp_frame"] = [r.status_code, r.mimetype, len(r.data)]
out["meta_sensors"] = len(c.get("/api/meta").get_json().get("sensors", []))
# read after the requests: the schema is detected on first connect
out["psd_kind"] = serve._psd_kind
out["pfp_kind"] = serve._pfp_kind
print("PROBE " + json.dumps(out))
'''


def run(argv, env_extra, cwd=None):
    env = {**os.environ, **env_extra}
    p = subprocess.run(argv, env=env, cwd=cwd or ROOT,
                       capture_output=True, text=True, timeout=600)
    return p.returncode, p.stdout + p.stderr


def probe(dbdir, tmp):
    """Import serve.py in a clean process and exercise the CBRS endpoints."""
    script = os.path.join(tmp, "probe.py")
    with open(script, "w") as f:
        f.write(PROBE)
    env = {"ATLAS_ROOT": ROOT, "SENSOR": SENSOR,
           "SPECTRUM_DB": os.path.join(dbdir, "spectrum.duckdb"),
           "PSD_DB": os.path.join(dbdir, "psd.duckdb"),
           "PFP_DB": os.path.join(dbdir, "pfp.duckdb"),
           "IQ_DB": os.path.join(dbdir, "iq.duckdb")}
    rc, out = run([sys.executable, script], env)
    for line in out.splitlines():
        if line.startswith("PROBE "):
            return json.loads(line[6:]), out
    return None, out


def main():
    print("CBRS ingest path test\n")
    tmp = tempfile.mkdtemp(prefix="atlas-ingest-test-")
    data = os.path.join(tmp, "data")
    dbs = os.path.join(tmp, "dbs")
    os.makedirs(dbs)
    env = {"SPECTRUM_DB": os.path.join(dbs, "spectrum.duckdb"),
           "PSD_DB": os.path.join(dbs, "psd.duckdb"),
           "PFP_DB": os.path.join(dbs, "pfp.duckdb")}
    try:
        make_data(data)

        # ---- discovery is layout-independent ----
        rc, out = run([sys.executable, os.path.join(ING, "psd_ingest.py"),
                       "--root", data, "--list"], env)
        check("psd_ingest --list finds the sensor in a nested layout",
              rc == 0 and SENSOR in out and "2 day(s)" in out,
              next((l.strip() for l in out.splitlines() if SENSOR in l), ""))

        # ---- the old crash: no spectrum.duckdb anywhere ----
        check("no spectrum.duckdb was built yet",
              not os.path.exists(env["SPECTRUM_DB"]))
        rc, out = run([sys.executable, os.path.join(ING, "psd_ingest.py"),
                       SENSOR, "--root", data], env)
        check("psd_ingest runs without spectrum.duckdb (was an IOException)",
              rc == 0 and "Traceback" not in out and "12 new captures" in out,
              next((l for l in out.splitlines() if "Done in" in l), out[-200:]))

        rc, out = run([sys.executable, os.path.join(ING, "pfp_ingest.py"),
                       SENSOR, "--root", data], env)
        check("pfp_ingest runs without spectrum.duckdb",
              rc == 0 and "Traceback" not in out and "8 new frames" in out,
              next((l for l in out.splitlines() if "Done in" in l), out[-200:]))

        # ---- the viewer renders with NO compaction step ----
        got, out = probe(dbs, tmp)
        ok = got is not None
        check("serve.py reads the uncompacted schema", ok and got["psd_kind"] == "rows"
              and got["pfp_kind"] == "rows", str(got and (got["psd_kind"], got["pfp_kind"])))
        if ok:
            check("psd_meta advertises the layer",
                  got["psd_meta"].get("has") is True, str(got["psd_meta"])[:90])
            check("psd_layer renders a tile (was HTTP 500)",
                  got.get("psd_layer", [0])[0] == 200
                  and got["psd_layer"][1] == "image/webp",
                  str(got.get("psd_layer")))
            check("pfp_meta advertises the layer and its channel",
                  got["pfp_meta"].get("has") is True
                  and got["pfp_meta"].get("freqs") == [3555.0e6],   # Hz, as ingested
                  str(got["pfp_meta"].get("freqs")))
            check("pfp_frame renders a tile",
                  got.get("pfp_frame", [0])[0] == 200, str(got.get("pfp_frame")))

        # ---- build_db.py, then compaction, then the same checks again ----
        rc, out = run([sys.executable, os.path.join(ING, "build_db.py"),
                       "--csv-dir", os.path.join(data, "mds2-test", "summaries")],
                      env)
        check("build_db builds the summary layer",
              rc == 0 and os.path.exists(env["SPECTRUM_DB"]), out.strip()[-120:])

        rc, out = run([sys.executable, os.path.join(ING, "compact_db.py"),
                       "--no-swap"], {**env, "ATLAS_DB_DIR": dbs})
        check("--no-swap leaves the build files and the live ones alone",
              rc == 0 and os.path.exists(os.path.join(dbs, "psd_c.duckdb"))
              and not os.path.exists(os.path.join(dbs, "psd.duckdb.bak")))

        # compaction reports whether its output is safe to install, and a
        # missing source is a False rather than a silent swap of nothing
        import compact_db                                    # noqa: E402
        compact_db.DB_DIR = dbs
        check("compact_psd reports success on a good database",
              compact_db.compact_psd() is True)
        check("swap_in is a no-op when there is no build file",
              compact_db.swap_in("nosuch") is False)
        compact_db.DB_DIR = os.path.join(tmp, "no-databases-here")
        check("a missing source compacts to False, so nothing is swapped",
              (compact_db.compact_spectrum(), compact_db.compact_psd(),
               compact_db.compact_pfp()) == (False, False, False))
        compact_db.DB_DIR = dbs

        rc, out = run([sys.executable, os.path.join(ING, "compact_db.py")],
                      {**env, "ATLAS_DB_DIR": dbs})
        swapped = ("psd.duckdb <- psd_c.duckdb" in out
                   and "pfp.duckdb <- pfp_c.duckdb" in out
                   and "spectrum.duckdb <- spectrum_c.duckdb" in out)
        check("compact_db swaps all three files in itself", rc == 0 and swapped)
        check("the originals are kept as .bak",
              all(os.path.exists(os.path.join(dbs, f"{n}.duckdb.bak"))
                  for n in ("spectrum", "psd", "pfp")))
        check("no stray *_c.duckdb is left behind",
              not any(f.endswith("_c.duckdb") for f in os.listdir(dbs)),
              str(sorted(os.listdir(dbs))))

        got, out = probe(dbs, tmp)
        ok = got is not None
        check("serve.py reads the compacted schema after the swap",
              ok and got["psd_kind"] == "chunk" and got["pfp_kind"] == "chunk",
              str(got and (got["psd_kind"], got["pfp_kind"])))
        if ok:
            check("psd_layer still renders after compaction",
                  got.get("psd_layer", [0])[0] == 200, str(got.get("psd_layer")))
            check("pfp_frame still renders after compaction",
                  got.get("pfp_frame", [0])[0] == 200, str(got.get("pfp_frame")))
            check("the summary layer sees the sensor",
                  got["meta_sensors"] == 1, str(got["meta_sensors"]))

        # ---- failures are loud ----
        empty = os.path.join(tmp, "empty")
        os.makedirs(empty)
        rc, out = run([sys.executable, os.path.join(ING, "psd_ingest.py"),
                       SENSOR, "--root", empty], env)
        check("an empty source directory fails loudly",
              rc != 0 and "looked for" in out and "Traceback" not in out,
              next((l for l in out.splitlines() if "looked for" in l), ""))

        rc, out = run([sys.executable, os.path.join(ING, "psd_ingest.py"),
                       SENSOR, "--root", os.path.join(tmp, "nope")], env)
        check("a missing source directory names the flag to fix it",
              rc != 0 and "--root" in out and "SEA_DATA_ROOT" in out
              and "Traceback" not in out)

        rc, out = run([sys.executable, os.path.join(ING, "psd_ingest.py"),
                       "Wrong-Sensor", "--root", data], env)
        check("an unknown sensor lists the available ones",
              rc != 0 and SENSOR in out and "Traceback" not in out,
              next((l.strip() for l in out.splitlines() if "Available" in l), ""))

        rc, out = run([sys.executable, os.path.join(ING, "psd_ingest.py"),
                       SENSOR, "--root", data, "--stat", "median"], env)
        check("a stat with no files says which stats exist",
              rc != 0 and "their stats are: max" in out)

        # a PSD export handed to build_db.py must not silently build nothing
        rc, out = run([sys.executable, os.path.join(ING, "build_db.py"),
                       "--csv-dir", os.path.join(data, "mds2-test", "sea")], env)
        check("build_db rejects non-Summaries CSVs with a reason",
              rc != 0 and "are Summaries exports" in out and "Traceback" not in out)

        rc, out = run([sys.executable, os.path.join(ING, "build_db.py"),
                       "--csv-dir", os.path.join(tmp, "nothing-here")], env)
        check("build_db with no CSVs explains how to get them",
              rc != 0 and "fetch.py" in out and "psd_ingest" in out)

        # an empty ingest must not leave a meta row the server would advertise
        import duckdb
        stub = os.path.join(dbs, "stub_psd.duckdb")
        con = duckdb.connect(stub)
        con.execute("CREATE TABLE psd (sensor VARCHAR, t DOUBLE, spec BLOB)")
        con.execute("""CREATE TABLE psd_meta (sensor VARCHAR, f0 DOUBLE, df DOUBLE,
            nf INT, qmin DOUBLE, qmax DOUBLE, t_min DOUBLE, t_max DOUBLE,
            captures BIGINT)""")
        con.execute("INSERT INTO psd_meta VALUES (?,0,0,0,0,0,NULL,NULL,0)", [SENSOR])
        con.close()
        env2 = {"ATLAS_ROOT": ROOT, "SENSOR": SENSOR, "PSD_DB": stub,
                "IQ_DB": os.path.join(dbs, "none.duckdb"),
                "SPECTRUM_DB": os.path.join(dbs, "none.duckdb"),
                "PFP_DB": os.path.join(dbs, "none.duckdb")}
        rc, out = run([sys.executable, os.path.join(tmp, "probe.py")], env2)
        line = next((l for l in out.splitlines() if l.startswith("PROBE ")), None)
        res = json.loads(line[6:]) if line else {}
        check("a null-range meta row is not advertised as a layer",
              res.get("psd_meta", {}).get("has") is False, str(res.get("psd_meta")))

        # ---- the zero-download demo builds every layer ----
        # serve.py tells anyone without CBRS databases to run make_sample.py.
        # That script built only iq.duckdb for most of its life, so following
        # the advice left the message on screen and the sensor dropdown empty.
        demo = os.path.join(tmp, "demo-dbs")
        os.makedirs(demo)
        denv = {"ATLAS_DB_DIR": demo,
                "SPECTRUM_DB": os.path.join(demo, "spectrum.duckdb"),
                "PSD_DB": os.path.join(demo, "psd.duckdb"),
                "PFP_DB": os.path.join(demo, "pfp.duckdb"),
                "IQ_DB": os.path.join(demo, "iq.duckdb")}
        rc, out = run([sys.executable, os.path.join(HERE, "make_sample.py")], denv)
        made = [n for n in ("iq.duckdb", "spectrum.duckdb", "psd.duckdb",
                            "pfp.duckdb")
                if os.path.exists(os.path.join(demo, n))]
        check("make_sample.py builds all four layers, not just the IQ one",
              rc == 0 and len(made) == 4, ", ".join(made) or "none")

        env3 = {**denv, "ATLAS_ROOT": ROOT, "SENSOR": "DEMO-Directional"}
        rc, out = run([sys.executable, os.path.join(tmp, "probe.py")], env3)
        line = next((l for l in out.splitlines() if l.startswith("PROBE ")), None)
        res = json.loads(line[6:]) if line else {}
        check("the demo leaves serve.py with CBRS enabled, not disabled",
              "not found -- CBRS monitoring disabled" not in out
              and res.get("meta_sensors", 0) == 2,
              f"{res.get('meta_sensors')} sensor(s)")
        check("the demo's PSD and PFP layers both render",
              res.get("psd_meta", {}).get("has") is True
              and res.get("pfp_meta", {}).get("has") is True
              and res.get("psd_layer", [0])[0] == 200
              and res.get("pfp_frame", [0])[0] == 200,
              f"psd={res.get('psd_layer')} pfp={res.get('pfp_frame')}")

        # Rebuilding on top of real CBRS data would delete spectrum.duckdb and
        # merge demo sensors into psd/pfp, so it has to refuse by default.
        rc, out = run([sys.executable, os.path.join(HERE, "make_sample.py"),
                       "--cbrs"], denv)
        check("a second run refuses to overwrite existing CBRS databases",
              rc != 0 and "Not overwriting them" in out and "--force" in out)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if failed:
        print(f"RESULT: FAIL ({failed} check(s) failed above)")
        return 1
    print("RESULT: PASS - the CBRS path works end to end, uncompacted and compacted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
