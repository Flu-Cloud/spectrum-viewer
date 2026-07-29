#!/usr/bin/env python3
"""
atlas.py: one command for the whole pipeline.

You should not have to know which of five ingest scripts matches the bytes you
have, nor what folder layout each one wants, nor whether to compact afterwards.
Tell this what you have and it works the rest out.

    python atlas.py setup             check the install and build the demo
    python atlas.py status            what is built, what to run next
    python atlas.py get <thing>       fetch if needed, ingest, done
    python atlas.py serve             start the viewer

<thing> can be any of:

    a folder or file already on disk   ./SEA-DATA, ~/Downloads/mds2-3177, capture.tdms
    a friendly name                    lte-uplink          (see datasets.json)
    a PDR record id or ark             mds2-3177, ark:/88434/mds2-3177
    a DOI or landing URL               https://doi.org/10.18434/mds2-3177
    any direct file URL                https://example.org/capture.sigmf-data

For a folder, every file in it is classified and the matching ingest runs:
SigMF/TDMS/npy captures, CBRS PSD exports, CBRS PFP exports, Summaries CSVs,
and prebuilt .duckdb databases are each recognised on sight. A record that
publishes finished .duckdb files is simply copied into place with no ingest at
all, which is the fastest path if one exists for your dataset.

The individual scripts under ingest/ all still work on their own; this drives
them, it does not replace them.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ING = os.path.join(HERE, "ingest")
sys.path.insert(0, ING)

DATASETS = os.path.join(HERE, "datasets.json")
DBS = ("spectrum", "psd", "pfp", "iq")
IQ_EXT = (".sigmf-meta", ".tdms", ".npy")

# Which table proves a database actually holds data, per schema.
SCHEMAS = {
    "spectrum": [("raw", "raw")],
    "psd": [("psd_chunk", "compacted"), ("psd", "uncompacted")],
    "pfp": [("pfp_chunk", "compacted"), ("pfp", "uncompacted")],
    "iq": [("iq_stft", "stft pyramid")],
}

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def db_path(name):
    env = {"spectrum": "SPECTRUM_DB", "psd": "PSD_DB",
           "pfp": "PFP_DB", "iq": "IQ_DB"}[name]
    return os.environ.get(env, os.path.join(HERE, f"{name}.duckdb"))


def short(p):
    """Repo-relative when that is actually shorter, absolute otherwise."""
    try:
        rel = os.path.relpath(p, HERE)
    except ValueError:              # different drive on Windows
        return p
    return rel if not rel.startswith("..") else p


def run(argv, dry=False):
    """Run one step, echoing it first so the transcript is reproducible."""
    shown = " ".join(("python" if a == sys.executable else
                      short(a) if a.startswith(HERE) else a)
                     for a in argv)
    print(f"\n$ {shown}", flush=True)
    if dry:
        return 0
    return subprocess.run(argv, cwd=HERE).returncode


def script(name, *args):
    return [sys.executable, os.path.join(ING, name), *[str(a) for a in args]]


def load_datasets():
    try:
        with open(DATASETS) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


# ---- classify what is on disk -----------------------------------------

def classify(root):
    """Walk root -> what kinds of data are in it, and where.

    Returns a dict of kind -> sorted list of directories (or files) to hand to
    the matching ingest script.
    """
    import psd_ingest
    import pfp_ingest

    out = {"iq": set(), "psd": set(), "pfp": set(), "csv": set(), "duckdb": []}
    if os.path.isfile(root):
        names = [(os.path.dirname(root) or ".", os.path.basename(root))]
    else:
        names = [(dp, n) for dp, _, ns in os.walk(root) for n in ns]
    for dirpath, n in names:
        low = n.lower()
        if low.endswith(IQ_EXT):
            out["iq"].add(root if os.path.isdir(root) else dirpath)
        elif low.endswith(".duckdb"):
            out["duckdb"].append(os.path.join(dirpath, n))
        elif pfp_ingest.NAME_RE.match(n):
            out["pfp"].add(root)
        elif psd_ingest.NAME_RE.match(n):
            out["psd"].add(root)
        elif low.endswith(".csv"):
            out["csv"].add(dirpath)
    out["duckdb"].sort()
    return out


def plan_for(root, found, compact):
    """-> (list of (label, argv), list of prebuilt db files to copy)."""
    import psd_ingest
    import pfp_ingest
    steps, prebuilt = [], []

    for p in found["duckdb"]:
        stem = os.path.basename(p)[:-len(".duckdb")].replace("_c", "")
        if stem in DBS:
            prebuilt.append((p, db_path(stem)))

    for d in sorted(found["iq"]):
        steps.append((f"IQ captures in {short(d)}",
                      script("iq_ingest.py", d, "--dataset",
                             os.path.basename(os.path.normpath(d)))))
    for d in sorted(found["psd"]):
        for s in sorted(psd_ingest.discover(d, "max")):
            steps.append((f"CBRS PSD, sensor {s}",
                          script("psd_ingest.py", s, "--root", d)))
    for d in sorted(found["pfp"]):
        for s in sorted(pfp_ingest.discover(d, "max_peak")):
            steps.append((f"CBRS PFP, sensor {s}",
                          script("pfp_ingest.py", s, "--root", d)))
    # Summaries CSVs are whatever is left over that is still a CSV.
    for d in sorted(found["csv"]):
        steps.append((f"CBRS Summaries in {short(d)}",
                      script("build_db.py", "--csv-dir", d)))
    if compact and any(k for k in ("psd", "pfp") if found[k]):
        steps.append(("compact the CBRS databases", script("compact_db.py")))
    return steps, prebuilt


# ---- subcommands ------------------------------------------------------

def cmd_status(args):
    import duckdb
    print(f"ATLAS in {HERE}\n")
    rows, present = [], set()
    for name in DBS:
        p = db_path(name)
        if not os.path.exists(p):
            rows.append((name, "absent", "", "", ""))
            continue
        try:
            c = duckdb.connect(p, read_only=True)
            tables = {r[0] for r in c.execute(
                "SELECT table_name FROM information_schema.tables").fetchall()}
            kind = next((lbl for t, lbl in SCHEMAS[name] if t in tables), "unknown")
            tbl = next((t for t, _ in SCHEMAS[name] if t in tables), None)
            n = c.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0] if tbl else 0
            span = ""
            if name in ("psd", "pfp") and f"{name}_meta" in tables:
                r = c.execute(f"SELECT min(t_min), max(t_max) FROM {name}_meta").fetchone()
                if r and r[0] is not None:
                    span = f"{(r[1]-r[0])/86400:.1f} days"
                else:
                    kind += " (no time range: nothing ingested)"
            if name == "iq" and "iq_meta" in tables:
                span = f"{c.execute('SELECT count(*) FROM iq_meta').fetchone()[0]} capture(s)"
            c.close()
            if n:
                present.add(name)
            rows.append((name, kind, f"{n:,} rows", span,
                         f"{os.path.getsize(p)/1e6:.1f} MB"))
        except Exception as e:                              # noqa: BLE001
            rows.append((name, f"unreadable: {e}", "", "", ""))

    w = max(len(r[1]) for r in rows) + 2
    for name, kind, n, span, size in rows:
        print(f"  {name + '.duckdb':18} {kind:{w}} {n:>14}  {span:>14}  {size:>10}")

    print()
    if not present:
        print("Nothing is built yet. Start with:\n"
              "    python atlas.py setup            # zero-download demo\n"
              "    python atlas.py get <folder>     # data you already have\n"
              "    python atlas.py get mds2-3177    # data from NIST PDR")
    else:
        print("Ready. Start the viewer with:\n    python atlas.py serve")
        if {"psd", "pfp"} & present and not any(
                os.path.exists(db_path(n) + ".bak") for n in ("psd", "pfp")):
            print("\nOptional, once the CBRS databases get large:\n"
                  "    python ingest/compact_db.py      # smaller and faster")
    return 0


def cmd_setup(args):
    missing = []
    for mod, pkg in [("duckdb", "duckdb"), ("numpy", "numpy"),
                     ("flask", "flask"), ("PIL", "Pillow")]:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"Missing dependencies: {', '.join(missing)}\n\n"
              "    python -m pip install -r requirements.txt\n\n"
              "On macOS or Linux, make a virtual environment first:\n"
              "    python -m venv .venv && source .venv/bin/activate\n"
              "(Windows PowerShell: .venv\\Scripts\\activate)", file=sys.stderr)
        return 1
    rc = run([sys.executable, os.path.join(HERE, "examples", "verify.py")],
             args.dry_run)
    if rc == 0 and not args.dry_run:
        print("\nSetup complete. Next:\n    python atlas.py serve")
    return rc


def cmd_serve(args):
    return run([sys.executable, os.path.join(HERE, "serve.py")], args.dry_run)


def cmd_get(args):
    target = args.target
    datasets = load_datasets()

    if target in datasets and not os.path.exists(target):
        entry = datasets[target]
        print(f"{target}: {entry.get('title', '')}")
        if entry.get("note"):
            print(f"  note: {entry['note']}")
        target = entry["record"]
        if entry.get("filter") and not args.filter:
            args.filter = entry["filter"]

    # Already on disk? Then there is nothing to download.
    if os.path.exists(target):
        root = os.path.abspath(target)
        print(f"using local data: {root}")
    else:
        root = os.path.abspath(args.dest or os.path.join(
            HERE, "downloads", os.path.basename(target.rstrip("/"))))
        argv = script("fetch.py", target, "--dest", root)
        if args.filter:
            argv += ["--filter", args.filter]
        argv += args.fetch_args
        rc = run(argv, args.dry_run)
        if rc != 0:
            print("\ndownload did not finish; nothing was ingested",
                  file=sys.stderr)
            return rc
        if args.dry_run:
            print("\n(dry run: stopping here, since what to ingest depends on "
                  "what the download produces)")
            return 0

    found = classify(root)
    steps, prebuilt = plan_for(root, found, args.compact)
    if not steps and not prebuilt:
        print(f"\nNothing recognisable under {root}.\n"
              "  Expected IQ captures (.sigmf-meta/.tdms/.npy), CBRS PSD or PFP\n"
              "  CSV exports, Summaries CSVs, or prebuilt .duckdb files.",
              file=sys.stderr)
        return 1

    print(f"\nplan for {root}:")
    for src, dst in prebuilt:
        print(f"  - install prebuilt {os.path.basename(dst)} "
              f"(no ingest needed)")
    for label, _ in steps:
        print(f"  - {label}")

    for src, dst in prebuilt:
        if args.dry_run:
            continue
        if os.path.exists(dst):
            shutil.move(dst, dst + ".bak")
            print(f"  kept the previous {os.path.basename(dst)} as "
                  f"{os.path.basename(dst)}.bak")
        shutil.copy2(src, dst)
        print(f"  installed {os.path.basename(dst)}")

    failures = []
    for label, argv in steps:
        if run(argv, args.dry_run) != 0:
            failures.append(label)

    if args.dry_run:
        return 0
    print()
    if failures:
        print(f"{len(failures)} step(s) failed: " + "; ".join(failures),
              file=sys.stderr)
    cmd_status(args)
    return 1 if failures else 0


def main():
    ap = argparse.ArgumentParser(
        description=__doc__.strip().splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="run 'python atlas.py get --help' for the download options")
    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("setup", help="check the install and build the demo")
    p.set_defaults(fn=cmd_setup)

    p = sub.add_parser("status", help="what is built, and what to run next")
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("serve", help="start the viewer")
    p.set_defaults(fn=cmd_serve)

    p = sub.add_parser("get", help="fetch if needed, then ingest whatever it is")
    p.add_argument("target", help="folder, file, dataset name, record id, or URL")
    p.add_argument("--dest", default=None,
                   help="where to download to (default downloads/<name>)")
    p.add_argument("--filter", default=None,
                   help="only files whose path contains this substring")
    p.add_argument("--compact", action="store_true",
                   help="also run compact_db.py when CBRS data was ingested")
    p.add_argument("fetch_args", nargs="*", default=[],
                   help="extra flags passed straight to fetch.py, e.g. --any-host")
    p.set_defaults(fn=cmd_get)

    for q in sub.choices.values():
        q.add_argument("--dry-run", action="store_true",
                       help="print the plan and change nothing")

    args = ap.parse_args()
    if not getattr(args, "cmd", None):
        ap.print_help()
        return 0
    return args.fn(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
