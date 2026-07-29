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
# Files that belong to a capture but are not the thing you point an
# ingest at. Counting these as "unrecognised" would be misleading.
COMPANION_EXT = (".sigmf-data", ".sha256")

# Directories a search never needs to descend into. Skipping these is the
# difference between scanning a home folder in seconds and in hours.
SKIP_DIRS = {
    "__pycache__", "node_modules", "site-packages", "venv", "env",
    "AppData", "Library", "Windows", "Program Files", "Program Files (x86)",
    "ProgramData", "$Recycle.Bin", "System Volume Information", "OneDriveTemp",
    "Application Data", "Local Settings", "Recovery", "PerfLogs", "tmp",
}

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

def looks_like_summaries(path):
    """A Summaries CSV names its columns. Metadata and readme CSVs do not, and
    handing those to build_db.py just produces a confusing failure."""
    try:
        with open(path, "r", errors="replace") as f:
            head = f.readline().lower()
    except OSError:
        return False
    return "sensor_name" in head and "channel_frequency_mhz" in head


_NAME_RES = None


def name_res():
    """(psd, pfp) filename patterns, loaded from the ingest scripts themselves
    so there is exactly one definition of what a CBRS export is called."""
    global _NAME_RES
    if _NAME_RES is None:
        import psd_ingest
        import pfp_ingest
        _NAME_RES = (psd_ingest.NAME_RE, pfp_ingest.NAME_RE)
    return _NAME_RES


def kind_of(dirpath, name):
    """What one file is: 'iq' | 'duckdb' | 'pfp' | 'psd' | 'summaries' | None.

    Everything is decided by extension, by filename shape, or by a CSV's own
    header. Nothing here depends on the folder a file happens to sit in.
    """
    psd_re, pfp_re = name_res()
    low = name.lower()
    if low.endswith(IQ_EXT):
        return "iq"
    if low.endswith(".duckdb"):
        return "duckdb"
    if pfp_re.match(name):
        return "pfp"
    if psd_re.match(name):
        return "psd"
    if low.endswith(".csv"):
        return "summaries" if looks_like_summaries(
            os.path.join(dirpath, name)) else None
    return None


def walk_limited(root, max_files=200000, max_depth=12):
    """Yield (dirpath, name) under root, skipping system and toolchain noise.

    Sets truncated[0] when the file budget runs out, so a search over a whole
    drive says so instead of quietly reporting a partial answer.
    """
    root = os.path.abspath(root)
    base = root.rstrip(os.sep).count(os.sep)
    truncated = [False]
    n = 0

    def gen():
        nonlocal n
        for dirpath, dirnames, filenames in os.walk(root, onerror=lambda e: None):
            if dirpath.count(os.sep) - base >= max_depth:
                dirnames[:] = []
            dirnames[:] = [d for d in dirnames
                           if d not in SKIP_DIRS and not d.startswith(".")]
            for name in filenames:
                yield dirpath, name
                n += 1
                if n >= max_files:
                    truncated[0] = True
                    return
    return gen(), truncated


def classify(root):
    """Walk root -> what kinds of data are in it, and where.

    Returns a dict of kind -> sorted list of directories (or files) to hand to
    the matching ingest script.
    """
    out = {"iq": set(), "psd": set(), "pfp": set(), "csv": set(),
           "duckdb": [], "other": []}
    # A single file means that file, not everything sitting beside it. The CBRS
    # ingests take a directory, so those get the file's parent; the IQ ingest
    # takes either, so it gets the file itself.
    one = os.path.isfile(root)
    if one:
        names = [(os.path.dirname(root) or ".", os.path.basename(root))]
    else:
        names = [(dp, n) for dp, _, ns in os.walk(root) for n in ns]
    for dirpath, n in names:
        kind = kind_of(dirpath, n)
        if kind == "iq":
            out["iq"].add(os.path.join(dirpath, n) if one else root)
        elif kind == "duckdb":
            out["duckdb"].append(os.path.join(dirpath, n))
        elif kind == "pfp":
            out["pfp"].add(dirpath if one else root)
        elif kind == "psd":
            out["psd"].add(dirpath if one else root)
        elif kind == "summaries":
            out["csv"].add(dirpath)
        elif n.lower().endswith(".csv"):
            out["other"].append(os.path.join(dirpath, n))
    out["duckdb"].sort()
    return out


def plan_for(root, found, compact):
    """-> (list of (label, argv), list of prebuilt db files to copy)."""
    import psd_ingest
    import pfp_ingest
    steps, prebuilt = [], []

    for p in found["duckdb"]:
        stem = os.path.basename(p)[:-len(".duckdb")]
        if stem.endswith("_c"):          # a compact_db.py build file
            stem = stem[:-2]
        if stem in DBS:
            prebuilt.append((p, db_path(stem)))

    for d in sorted(found["iq"]):
        named = d if os.path.isdir(d) else os.path.dirname(d)
        steps.append((f"IQ captures in {short(d)}",
                      script("iq_ingest.py", d, "--dataset",
                             os.path.basename(os.path.normpath(named)))))
    for d in sorted(found["psd"]):
        for s in sorted(psd_ingest.discover(d, "max")):
            steps.append((f"CBRS PSD, sensor {s}, under {short(d)}",
                          script("psd_ingest.py", s, "--root", d)))
    for d in sorted(found["pfp"]):
        for s in sorted(pfp_ingest.discover(d, "max_peak")):
            steps.append((f"CBRS PFP, sensor {s}, under {short(d)}",
                          script("pfp_ingest.py", s, "--root", d)))
    # Summaries CSVs are whatever is left over that is still a CSV.
    for d in sorted(found["csv"]):
        steps.append((f"CBRS Summaries in {short(d)}",
                      script("build_db.py", "--csv-dir", d)))
    if compact and any(k for k in ("psd", "pfp") if found[k]):
        steps.append(("compact the CBRS databases", script("compact_db.py")))
    return steps, prebuilt


# ---- searching for data you already have ------------------------------

def default_roots():
    """Where a spectrum download plausibly landed on someone's machine."""
    home = os.path.expanduser("~")
    cand = [os.environ.get("SEA_DATA_ROOT"),
            os.path.join(HERE, "SEA-DATA"), os.path.join(HERE, "iqdata"),
            os.path.join(HERE, "downloads"), os.path.join(HERE, "data"),
            os.path.join(home, "Downloads"), os.path.join(home, "Desktop"),
            os.path.join(home, "Documents")]
    out = []
    for p in cand:
        if p and os.path.isdir(p) and os.path.abspath(p) not in out:
            out.append(os.path.abspath(p))
    return out


def deep_roots():
    """Everything in default_roots, plus the whole home folder and, on
    Windows, every other fixed drive. Slow on purpose."""
    out = list(default_roots())
    home = os.path.abspath(os.path.expanduser("~"))
    if home not in out:
        out.append(home)
    if os.name == "nt":
        import string
        for letter in string.ascii_uppercase:
            drive = f"{letter}:\\"
            if letter != "C" and os.path.isdir(drive):
                out.append(drive)
    return out


def survey(root, max_files):
    """Inventory one directory: what is recognised, and what is not."""
    import collections
    found = {"iq": 0, "psd": 0, "pfp": 0, "summaries": 0, "duckdb": []}
    sensors, days, unknown = set(), set(), collections.Counter()
    psd_re, pfp_re = name_res()
    files = 0
    gen, truncated = walk_limited(root, max_files=max_files)
    for dirpath, name in gen:
        files += 1
        kind = kind_of(dirpath, name)
        if kind == "duckdb":
            found["duckdb"].append(os.path.join(dirpath, name))
        elif kind in ("psd", "pfp"):
            found[kind] += 1
            m = (psd_re if kind == "psd" else pfp_re).match(name)
            sensors.add(m.group("sensor"))
            days.add(m.group("day"))
        elif kind:
            found[kind] += 1
        elif not name.lower().endswith(COMPANION_EXT):
            unknown[os.path.splitext(name)[1].lower() or "(no extension)"] += 1
    return {"root": root, "found": found, "sensors": sorted(sensors),
            "days": sorted(days), "unknown": unknown, "files": files,
            "truncated": truncated[0]}


def report(s):
    """Print one directory's inventory. Returns True if anything was usable."""
    f, any_hit = s["found"], False
    print(f"\n  {s['root']}")
    if f["iq"]:
        print(f"      {f['iq']:>7} IQ capture file(s)")
        any_hit = True
    for kind, label in (("psd", "CBRS PSD export(s)"),
                        ("pfp", "CBRS PFP export(s)")):
        if f[kind]:
            span = (f"{s['days'][0]} .. {s['days'][-1]}" if s["days"] else "")
            print(f"      {f[kind]:>7} {label:<22} "
                  f"{len(s['sensors'])} sensor(s)  {span}")
            any_hit = True
    if f["summaries"]:
        print(f"      {f['summaries']:>7} CBRS Summaries CSV(s)")
        any_hit = True
    for p in f["duckdb"]:
        stem = os.path.basename(p)[:-len(".duckdb")].rstrip("_c")
        mark = "prebuilt database" if stem in DBS else "database (not one of ours)"
        print(f"      {'':>7} {mark}: {os.path.basename(p)}")
        any_hit = any_hit or stem in DBS
    if s["sensors"]:
        print(f"      sensors: {', '.join(s['sensors'])}")
    unknown = sum(s["unknown"].values())
    if unknown:
        top = ", ".join(f"{ext} {n}" for ext, n in s["unknown"].most_common(6))
        print(f"      {unknown:>7} file(s) not recognised  ({top})")
    if not any_hit and not unknown:
        print("           nothing here")
    if s["truncated"]:
        print("      NOTE: hit the file limit; re-run with a bigger --max-files "
              "or point --root at a narrower folder")
    return any_hit


def cmd_scan(args):
    """Find spectrum data anywhere on this machine and say what it is."""
    roots = args.roots or (deep_roots() if args.deep else default_roots())
    if not roots:
        print("No obvious place to look. Pass a folder: "
              "python atlas.py scan C:\\path\\to\\data", file=sys.stderr)
        return 1
    print(f"searching {len(roots)} place(s), up to {args.max_files:,} files each."
          + ("" if args.deep else "  Add --deep to search wider, or name a "
                                  "folder to search exactly that."))
    missing = [r for r in roots if not os.path.isdir(r)]
    roots = [r for r in roots if os.path.isdir(r)]
    for r in roots:
        print(f"  - {r}")
    for r in missing:
        print(f"  - {r}   (does not exist, skipped)")
    if not roots:
        print("\nNone of those folders exist.", file=sys.stderr)
        return 1

    hits, empty, csv_missed = [], [], 0
    for r in roots:
        s = survey(r, args.max_files)
        if report(s):
            hits.append(s)
        else:
            empty.append(r)
        csv_missed += s["unknown"].get(".csv", 0)

    print()
    if csv_missed:
        # The most likely way this misses real data: exports named differently.
        print(f"{csv_missed} .csv file(s) were found but not recognised. CBRS "
              "exports are matched by name:")
        print("    PSD        <YYYY-MM-DD>_<sensor>_<stat>.csv")
        print("    PFP        PFP_<YYYY-MM-DD>_<sensor>_<stat>.csv")
        print("    Summaries  any .csv whose header has sensor_name and "
              "channel_frequency_mhz")
        print("  If yours look different, rename them to match or say so and "
              "the pattern can be widened.\n")
    if not hits:
        print("Found nothing to ingest. Either the data is somewhere not "
              "searched (pass the folder directly, or use --deep), or it is "
              "named differently than the patterns above.")
        return 1
    print("To ingest what was found, run:")
    for s in hits:
        print(f'    python atlas.py get "{s["root"]}"')
    if empty:
        print(f"\n(nothing in: {', '.join(empty)})")
    return 0


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
              "    python atlas.py scan             # find data already on this machine\n"
              "    python atlas.py get <folder>     # ingest a folder you have\n"
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
        if entry.get("filter") and args.filter is None:
            args.filter = entry["filter"]

    # Already on disk? Then there is nothing to download.
    if os.path.exists(target):
        root = os.path.abspath(target)
        print(f"using local data: {root}")
    else:
        # Derive the download folder from the PARSED source, so a pasted
        # "<https://.../mds2-3177>" does not create a directory named
        # "mds2-3177>". fetch.py cleans the argument the same way.
        import fetch
        try:
            kind, val = fetch.parse_source(target)
            name = val if kind == "record" else os.path.basename(
                os.path.normpath(fetch.urllib.parse.urlsplit(val).path)) or "download"
        except Exception:                                   # noqa: BLE001
            name = os.path.basename(target.rstrip("/")) or "download"
        root = os.path.abspath(args.dest or os.path.join(HERE, "downloads", name))
        argv = script("fetch.py", fetch.clean_source(target), "--dest", root)
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
        if found["other"]:
            print(f"  {len(found['other'])} CSV file(s) are there but none has a "
                  "Summaries header (sensor_name, channel_frequency_mhz):",
                  file=sys.stderr)
            for f in found["other"][:5]:
                print(f"    {short(f)}", file=sys.stderr)
        return 1

    print(f"\nplan for {root}:")
    for src, dst in prebuilt:
        print(f"  - install prebuilt {os.path.basename(dst)} "
              f"(no ingest needed)")
    for label, _ in steps:
        print(f"  - {label}")
    if found["other"]:
        # Always say what was passed over, so data named unexpectedly cannot be
        # skipped without the user seeing it.
        print(f"  ({len(found['other'])} CSV file(s) ignored, no Summaries "
              f"header; e.g. {short(found['other'][0])})")

    if args.ask and not args.dry_run:
        try:
            if input("\nproceed? [y/N] ").strip().lower() not in ("y", "yes"):
                print("nothing done")
                return 0
        except EOFError:
            print("\nno terminal to ask on; re-run without --ask")
            return 1

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

    p = sub.add_parser("scan", help="search this machine for spectrum data")
    p.add_argument("roots", nargs="*", default=None,
                   help="folders to search (default: the usual download spots)")
    p.add_argument("--deep", action="store_true",
                   help="also search your home folder and every other drive")
    p.add_argument("--max-files", type=int, default=200000,
                   help="file budget per folder (default 200000)")
    p.set_defaults(fn=cmd_scan)

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
    p.add_argument("--ask", action="store_true",
                   help="show the plan and wait for confirmation before running")
    p.epilog = ("any other flag is passed straight to ingest/fetch.py, "
                "e.g. --nist-only, --allow-host HOST, --allow-http")
    p.set_defaults(fn=cmd_get)

    for q in sub.choices.values():
        q.add_argument("--dry-run", action="store_true",
                       help="print the plan and change nothing")

    args, extra = ap.parse_known_args()
    if not getattr(args, "cmd", None):
        ap.print_help()
        return 0
    # Unknown flags are forwarded to fetch.py (--nist-only, --allow-host, ...);
    # anywhere else they are a typo and should not be swallowed.
    if extra and args.cmd != "get":
        ap.error(f"unrecognized arguments: {' '.join(extra)}")
    args.fetch_args = extra
    return args.fn(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
