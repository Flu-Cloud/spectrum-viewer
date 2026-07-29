"""test_atlas.py: prove `python atlas.py get <thing>` works it out for you.

The point of atlas.py is that a user should not have to know which of five
ingest scripts matches the bytes they have. This builds one folder containing
IQ captures, CBRS PSD exports, CBRS PFP exports and Summaries CSVs all mixed
together, hands the folder to `atlas.py get`, and checks that every layer ends
up built without naming a single ingest script.

Also covers: status on a fresh clone, --dry-run changing nothing, prebuilt
.duckdb files being installed instead of ingested, a folder holding nothing
recognisable, and friendly names from datasets.json.

    python examples/test_atlas.py          # exits 0 = PASS
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ATLAS = os.path.join(ROOT, "atlas.py")
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)

import test_ingest as TI                                     # noqa: E402

failed = 0


def check(name, ok, detail=""):
    global failed
    if not ok:
        failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{(' - ' + detail) if detail else ''}")
    return ok


def make_iq(folder, n=1024 * 64):
    """A tiny SigMF capture. Default size is enough for one STFT level."""
    import numpy as np
    os.makedirs(folder, exist_ok=True)
    t = np.arange(n) / 2e6
    x = (np.exp(2j * np.pi * 3e5 * t) * 0.5).astype(np.complex64)
    x.tofile(os.path.join(folder, "demo.sigmf-data"))
    with open(os.path.join(folder, "demo.sigmf-meta"), "w") as f:
        json.dump({"global": {"core:datatype": "cf32_le",
                              "core:sample_rate": 2e6, "core:version": "1.0.0"},
                   "captures": [{"core:sample_start": 0,
                                 "core:frequency": 2.412e9}],
                   "annotations": []}, f)


def run(args, dbs, extra=None):
    env = {**os.environ,
           "SPECTRUM_DB": os.path.join(dbs, "spectrum.duckdb"),
           "PSD_DB": os.path.join(dbs, "psd.duckdb"),
           "PFP_DB": os.path.join(dbs, "pfp.duckdb"),
           "IQ_DB": os.path.join(dbs, "iq.duckdb"),
           "ATLAS_DB_DIR": dbs, **(extra or {})}
    # stdin closed: --ask must not be able to block a non-interactive run
    p = subprocess.run([sys.executable, ATLAS] + args, env=env, cwd=ROOT,
                       stdin=subprocess.DEVNULL, capture_output=True,
                       text=True, timeout=900)
    return p.returncode, p.stdout + p.stderr


def main():
    print("atlas.py test\n")
    tmp = tempfile.mkdtemp(prefix="atlas-cli-test-")
    data = os.path.join(tmp, "everything")
    dbs = os.path.join(tmp, "dbs")
    os.makedirs(dbs)
    try:
        # one folder, four different kinds of data mixed together
        TI.make_data(data)
        make_iq(os.path.join(data, "mds2-test", "iq", "run1"))

        rc, out = run(["status"], dbs)
        check("status on a fresh clone says nothing is built",
              rc == 0 and "Nothing is built yet" in out and "absent" in out)

        rc, out = run(["get", data, "--dry-run"], dbs)
        check("--dry-run prints a plan naming every kind found",
              rc == 0 and "IQ captures in" in out and "CBRS PSD, sensor" in out
              and "CBRS PFP, sensor" in out and "CBRS Summaries in" in out,
              str(out.count("  - ")) + " planned step(s)")
        check("--dry-run builds nothing", not os.listdir(dbs))

        rc, out = run(["get", data], dbs)
        built = sorted(f for f in os.listdir(dbs) if f.endswith(".duckdb"))
        check("get on a mixed folder builds every layer, no script named",
              rc == 0 and built == ["iq.duckdb", "pfp.duckdb", "psd.duckdb",
                                    "spectrum.duckdb"], str(built))
        check("it finishes by reporting the new state",
              "Ready. Start the viewer with" in out)
        check("the sensor was discovered, not supplied",
              TI.SENSOR in out, TI.SENSOR)

        rc, out = run(["status"], dbs)
        check("status now shows each layer with its schema and size",
              rc == 0 and "uncompacted" in out and "stft pyramid" in out
              and "1 capture(s)" in out and "days" in out)

        # re-running must be safe: every ingest is resumable
        rc, out = run(["get", data], dbs)
        check("re-running get is idempotent",
              rc == 0 and ("already ingested" in out or "skip (already" in out))

        # prebuilt databases: install, do not ingest
        pre = os.path.join(tmp, "prebuilt")
        os.makedirs(pre)
        shutil.copy2(os.path.join(dbs, "psd.duckdb"),
                     os.path.join(pre, "psd.duckdb"))
        dbs2 = os.path.join(tmp, "dbs2")
        os.makedirs(dbs2)
        rc, out = run(["get", pre], dbs2)
        check("a folder of prebuilt .duckdb files skips ingest entirely",
              rc == 0 and "no ingest needed" in out
              and os.path.exists(os.path.join(dbs2, "psd.duckdb"))
              and "psd_ingest.py" not in out)

        rc, out = run(["get", pre], dbs2)
        check("re-installing a prebuilt database keeps the old one as .bak",
              rc == 0 and os.path.exists(os.path.join(dbs2, "psd.duckdb.bak")))

        empty = os.path.join(tmp, "empty")
        os.makedirs(empty)
        rc, out = run(["get", empty], dbs)
        check("a folder with nothing recognisable fails and says what it wanted",
              rc != 0 and "Nothing recognisable" in out
              and "Traceback" not in out)

        # friendly names resolve without touching the network (dry run)
        names = json.load(open(os.path.join(ROOT, "datasets.json")))
        real = [k for k in names if not k.startswith("_")]
        check("datasets.json parses and has entries", bool(real), str(real))
        rc, out = run(["get", "lte-uplink", "--dry-run"], dbs)
        check("a friendly name resolves to its record and default filter",
              rc == 0 and "mds2-3177" in out and "1.4MHz/config_0" in out
              and "fetch.py" in out,
              next((l.strip() for l in out.splitlines() if "fetch.py" in l), ""))

        rc, out = run(["get", "no-such-name-here", "--dry-run"], dbs)
        check("an unknown name is passed through to fetch, not crashed on",
              rc == 0 and "no-such-name-here" in out and "Traceback" not in out)

        rc, out = run(["get", "lte-uplink", "--filter", "", "--dry-run"], dbs)
        check("an explicit empty --filter beats the dataset default",
              rc == 0 and "--filter" not in out.split("$ ")[-1],
              next((l.strip() for l in out.splitlines() if "fetch.py" in l), ""))

        rc, out = run(["get", "mds2-3177", "--dry-run", "--nist-only",
                       "--allow-host", "example.org"], dbs)
        check("unknown flags are forwarded to fetch.py",
              rc == 0 and "--nist-only" in out and "--allow-host example.org" in out,
              next((l.strip() for l in out.splitlines() if "fetch.py" in l), ""))

        rc, out = run(["get", "<https://data.nist.gov/od/id/mds2-3177>",
                       "--dry-run"], dbs)
        check("a URL pasted with angle brackets still names the folder cleanly",
              rc == 0 and "--dest downloads/mds2-3177" in out
              and ">" not in out,
              next((l.strip() for l in out.splitlines() if "fetch.py" in l), ""))

        dbs5 = os.path.join(tmp, "dbs5")
        os.makedirs(dbs5)
        rc, out = run(["get", data, "--ask"], dbs5)
        check("--ask with no terminal refuses rather than running blind",
              rc != 0 and "no terminal" in out and not os.listdir(dbs5))

        rc, out = run(["status", "--not-a-flag"], dbs)
        check("a typo on another subcommand is still an error",
              rc != 0 and "unrecognized arguments" in out)

        # a metadata CSV must not be mistaken for a Summaries export
        junk = os.path.join(tmp, "junk")
        os.makedirs(junk)
        with open(os.path.join(junk, "file_manifest.csv"), "w") as f:
            f.write("filename,checksum\nx.tdms,abc\n")
        rc, out = run(["get", junk], dbs)
        check("a non-Summaries CSV is reported, not handed to build_db",
              rc != 0 and "none has a Summaries header" in out
              and "build_db.py" not in out, "")

        # ---- the no-argument path ----
        dbs8 = os.path.join(tmp, "dbs8")
        os.makedirs(dbs8)
        rc, out = run([], dbs8)
        check("bare 'python atlas.py' reports the situation without being asked",
              rc == 0 and "checking this machine" in out
              and "nothing built yet" in out and "disk" in out)
        check("with no terminal it recommends rather than guessing",
              "Recommended:" in out and "Choose 1" not in out)
        check("a no-terminal run changes nothing", not os.listdir(dbs8))

        menu = os.path.join(tmp, "menu.py")
        with open(menu, "w") as f:
            f.write(
                "import sys, builtins\n"
                f"sys.path.insert(0, {ROOT!r})\n"
                "import atlas\n"
                "sys.stdin = type('T', (), {'isatty': lambda s: True})()\n"
                "builtins.input = lambda p='': sys.argv[2]\n"
                "atlas.default_roots = lambda: [sys.argv[1]]\n"
                "sys.exit(atlas.cmd_auto(atlas.ns()))\n")

        env8 = {"SPECTRUM_DB": os.path.join(dbs8, "spectrum.duckdb"),
                "PSD_DB": os.path.join(dbs8, "psd.duckdb"),
                "PFP_DB": os.path.join(dbs8, "pfp.duckdb"),
                "IQ_DB": os.path.join(dbs8, "iq.duckdb"),
                "ATLAS_DB_DIR": dbs8}
        p = subprocess.run([sys.executable, menu, data, "q"],
                           env={**os.environ, **env8}, cwd=ROOT,
                           capture_output=True, text=True, timeout=900)
        mo = p.stdout + p.stderr
        check("with a terminal it offers a numbered menu",
              "What would you like to do?" in mo and "1)" in mo
              and "<- recommended" in mo)
        check("the recommendation is to load the data it found",
              "1) Load the data in" in mo,
              next((l.strip() for l in mo.splitlines() if l.strip().startswith("1)")), ""))
        check("quitting the menu does nothing",
              "nothing done" in mo and not os.listdir(dbs8))

        p = subprocess.run([sys.executable, menu, data, "1"],
                           env={**os.environ, **env8}, cwd=ROOT,
                           capture_output=True, text=True, timeout=900)
        built8 = sorted(f for f in os.listdir(dbs8) if f.endswith(".duckdb"))
        check("choosing 1 runs the whole ingest",
              p.returncode == 0 and built8 == ["iq.duckdb", "pfp.duckdb",
                                               "psd.duckdb", "spectrum.duckdb"],
              str(built8))

        # ---- doctor: one command on a hostile machine ----
        import duckdb
        legacy = os.path.join(tmp, "legacyrepo")
        os.makedirs(legacy)
        # a real PSD database under a name nothing would match on
        lp = os.path.join(legacy, "spectrum_viewer.db")
        con = duckdb.connect(lp)
        con.execute("CREATE TABLE psd (sensor VARCHAR, t DOUBLE, spec BLOB)")
        con.executemany("INSERT INTO psd VALUES (?,?,?)",
                        [("LEGACY", 1.7e9 + i * 240, bytes(2250)) for i in range(8)])
        con.execute("""CREATE TABLE psd_meta (sensor VARCHAR, f0 DOUBLE, df DOUBLE,
            nf INT, qmin DOUBLE, qmax DOUBLE, t_min DOUBLE, t_max DOUBLE,
            captures BIGINT)""")
        con.execute("INSERT INTO psd_meta VALUES ('LEGACY',3530040000.0,80000.0,"
                    "2250,-180.0,-90.0,1.7e9,1700001920.0,8)")
        con.close()
        with open(os.path.join(legacy, "notes.db"), "w") as f:
            f.write("not a database")

        import atlas                                        # noqa: E402
        name, label, rows = atlas.identify_db(lp)
        check("a database is identified by its tables, not its filename",
              (name, rows) == ("psd", 8), f"{name} / {label} / {rows} rows")
        name2, why, _ = atlas.identify_db(os.path.join(legacy, "notes.db"))
        check("a file that only looks like a database is rejected with a reason",
              name2 is None and "not readable" in why, why[:50])

        dbs6 = os.path.join(tmp, "dbs6")
        os.makedirs(dbs6)
        shutil.copy2(lp, os.path.join(dbs6, "spectrum_viewer.db"))
        shutil.copy2(os.path.join(legacy, "notes.db"),
                     os.path.join(dbs6, "notes.db"))

        rc, out = run(["doctor", data], dbs6)
        check("doctor spots an oddly-named database and says what it holds",
              "spectrum_viewer.db holds psd data" in out
              and "re-run with --adopt" in out,
              next((l.strip() for l in out.splitlines() if "holds psd" in l), ""))
        check("doctor ignores a file that only looks like a database",
              "notes.db ignored" in out)
        check("an unadopted database is left where it was",
              not os.path.exists(os.path.join(dbs6, "psd.duckdb")))

        rc2, out2 = run(["doctor", "--adopt", data], dbs6)
        check("doctor --adopt installs it where serve.py looks",
              "adopted spectrum_viewer.db as psd.duckdb" in out2
              and os.path.exists(os.path.join(dbs6, "psd.duckdb")))
        real, _lbl, nrows = atlas.identify_db(os.path.join(dbs6, "psd.duckdb"))
        check("the adopted database really is the PSD data",
              (real, nrows) == ("psd", 8), f"{real} / {nrows} rows")
        check("doctor checks environment, deps, disk, databases and data",
              rc == 0 and "1. Python and environment" in out
              and "2. Dependencies" in out and "3. Disk space" in out
              and "4. Databases" in out and "5. Spectrum data" in out
              and "7. End-to-end verification" in out)
        check("doctor builds the demo and verifies it end to end",
              "RESULT: PASS" in out and "VERDICT: working" in out)
        check("doctor finds source data and prints the command to load it",
              f'python atlas.py get "{data}"' in out)
        check("doctor reports the free space it checked",
              "free" in out and "Disk space" in out)

        rc, out = run(["doctor", "--dry-run"], dbs6)
        check("doctor --dry-run changes nothing",
              rc == 0 and "nothing will change" in out)

        # ---- scan: find data without being told where it is ----
        before_scan = sorted(os.listdir(dbs))
        junkdir = os.path.join(tmp, "scanjunk")
        os.makedirs(junkdir)
        with open(os.path.join(junkdir, "notes.txt"), "w") as f:
            f.write("hi")
        # a CBRS export named the wrong way round: the realistic miss case
        with open(os.path.join(junkdir, "SENSOR_2024-01-01_max.csv"), "w") as f:
            f.write("a,b\n1,2\n")

        rc, out = run(["scan", data, junkdir], dbs)
        check("scan finds data buried in a folder layout it was never told",
              rc == 0 and "CBRS PSD export(s)" in out
              and "CBRS PFP export(s)" in out and "IQ capture file(s)" in out,
              next((l.strip() for l in out.splitlines() if "PSD export" in l), ""))
        check("scan names the sensors it found",
              TI.SENSOR in out, TI.SENSOR)
        check("scan reports files it did not recognise",
              "not recognised" in out and ".txt" in out)
        check("scan warns loudly about unmatched CSVs and shows the patterns",
              "were found but not recognised" in out
              and "<YYYY-MM-DD>_<sensor>_<stat>.csv" in out)
        check("scan ends with the exact command to run",
              f'python atlas.py get "{data}"' in out)
        check("scan does not count a .sigmf-data companion as unrecognised",
              ".sigmf-data" not in out)

        rc, out = run(["scan", os.path.join(tmp, "does-not-exist")], dbs)
        check("scan says when a folder does not exist",
              rc != 0 and "does not exist" in out)

        rc, out = run(["scan", junkdir], dbs)
        check("scan exits non-zero when it finds nothing usable",
              rc != 0 and "Found nothing to ingest" in out)

        check("scan changed nothing", sorted(os.listdir(dbs)) == before_scan)

        # a single FILE means that file, not everything sitting beside it
        pair = os.path.join(tmp, "pair")
        make_iq(pair)
        for ext in ("sigmf-meta", "sigmf-data"):
            shutil.copy2(os.path.join(pair, f"demo.{ext}"),
                         os.path.join(pair, f"sibling.{ext}"))
        dbs3 = os.path.join(tmp, "dbs3")
        os.makedirs(dbs3)
        rc, out = run(["get", os.path.join(pair, "demo.sigmf-meta")], dbs3)
        check("one capture file ingests one capture, not its siblings",
              rc == 0 and "1 capture file(s)" in out and "1 ingested" in out
              and "--dataset pair" in out,
              next((l.strip() for l in out.splitlines() if "ingested" in l), ""))

        one_csv = os.path.join(data, "mds2-test", "sea", "spectra", "level-1",
                               f"{TI.DAYS[0]}_{TI.SENSOR}_max.csv")
        dbs4 = os.path.join(tmp, "dbs4")
        os.makedirs(dbs4)
        rc, out = run(["get", one_csv], dbs4)
        check("a single CBRS CSV is recognised, not rejected as unknown",
              rc == 0 and "CBRS PSD, sensor" in out
              and os.path.exists(os.path.join(dbs4, "psd.duckdb")),
              next((l.strip() for l in out.splitlines() if "  - " in l), ""))

        # a capture too short for an STFT must say so, and the failure must
        # reach the caller instead of looking like a clean run
        tiny = os.path.join(tmp, "tinyiq")
        make_iq(tiny, n=512)
        rc, out = run(["get", tiny], dbs)
        check("a capture too short for an STFT explains itself and fails",
              rc != 0 and "too short" in out and "at least 1024" in out
              and "Nothing was ingested" in out,
              next((l.strip() for l in out.splitlines() if "ERR " in l), ""))

        # a renamed ATLAS database IS recognised, by its tables
        stray = os.path.join(tmp, "stray")
        os.makedirs(stray)
        shutil.copy2(os.path.join(dbs, "psd.duckdb"),
                     os.path.join(stray, "my_capture.duckdb"))
        dbs7 = os.path.join(tmp, "dbs7")
        os.makedirs(dbs7)
        rc, out = run(["get", stray], dbs7)
        check("a renamed ATLAS database is recognised by its tables",
              rc == 0 and "no ingest needed" in out
              and os.path.exists(os.path.join(dbs7, "psd.duckdb")))

        # a database that is not ours must NOT be installed
        import duckdb as _dd
        foreign = os.path.join(tmp, "foreign")
        os.makedirs(foreign)
        fc = _dd.connect(os.path.join(foreign, "psd.duckdb"))
        fc.execute("CREATE TABLE customers (id INT, name VARCHAR)")
        fc.close()
        rc, out = run(["get", foreign], dbs7)
        check("a foreign database is refused even when named like ours",
              rc != 0 and "Nothing recognisable" in out)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if failed:
        print(f"RESULT: FAIL ({failed} check(s) failed above)")
        return 1
    print("RESULT: PASS - one command handles data you have and data you fetch")
    return 0


if __name__ == "__main__":
    sys.exit(main())
