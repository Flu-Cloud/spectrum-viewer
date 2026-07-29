"""
cbrs_files.py: finding CBRS export CSVs on disk.

psd_ingest.py and pfp_ingest.py differ only in the filename pattern they look
for and the shape of the numbers inside. Walking a directory, grouping matches
by sensor and day, and explaining an empty result are the same job in both, so
they live here once. Each caller passes its own compiled pattern.

A matching filename must yield named groups `day`, `sensor` and `stat`.
"""

import os
import sys


def walk_ext(root, exts):
    """Every file under root whose name ends in one of exts, sorted.

    A file path is returned as itself, so callers can accept either a folder
    or a single file without special-casing it.
    """
    if os.path.isfile(root):
        return [root] if root.lower().endswith(exts) else []
    out = []
    for dirpath, _, names in os.walk(root):
        for n in names:
            if n.lower().endswith(exts):
                out.append(os.path.join(dirpath, n))
    return sorted(out)


def discover(root, name_re, stat=None):
    """Walk root -> {sensor: {day: path}} for matching CSVs, any layout.

    The folder structure is deliberately ignored: the same call works on a
    download that preserved a record's directories, on a flat dump, and on the
    original Box share layout.
    """
    found = {}
    for dirpath, _, names in os.walk(root):
        for n in names:
            m = name_re.match(n)
            if not m or (stat and m.group("stat") != stat):
                continue
            found.setdefault(m.group("sensor"), {})[m.group("day")] = \
                os.path.join(dirpath, n)
    return found


def stats_present(root, name_re):
    """Which statistics exist on disk, e.g. ['max', 'mean']."""
    out = set()
    for _, _, names in os.walk(root):
        for n in names:
            m = name_re.match(n)
            if m:
                out.add(m.group("stat"))
    return sorted(out)


def sample_of(root, limit=8):
    """A few real filenames under root, to show what the layout actually is."""
    out = []
    for dirpath, _, names in os.walk(root):
        for n in sorted(names)[:limit]:
            out.append(os.path.relpath(os.path.join(dirpath, n), root))
            if len(out) >= limit:
                return out
    return out


def no_data(root, stat, name_re, pattern, label):
    """Explain an empty discovery instead of silently ingesting nothing.

    Returns 1 so callers can `sys.exit(no_data(...))`.
    """
    print(f"No {label} CSVs found under {os.path.abspath(root)}", file=sys.stderr)
    print(f"  looked for: {pattern}"
          + (f" with stat '{stat}'" if stat else "") + " (searched recursively)",
          file=sys.stderr)
    other = stats_present(root, name_re)
    if stat and other:
        print(f"  files with that name shape exist, but their stats are: "
              f"{', '.join(other)}. Try --stat {other[0]}.", file=sys.stderr)
    else:
        found = sample_of(root)
        if found:
            print("  what is actually there:", file=sys.stderr)
            for f in found:
                print(f"    {f}", file=sys.stderr)
        else:
            print("  that directory is empty.", file=sys.stderr)
    print("  Point --root at your copy of the data, or set SEA_DATA_ROOT "
          "(PowerShell: $env:SEA_DATA_ROOT=\"...\").", file=sys.stderr)
    return 1


def missing_root(root):
    """The message for a --root that does not exist at all."""
    return (f"source directory not found: {os.path.abspath(root)}\n"
            "  Pass --root /path/to/your/data, or set SEA_DATA_ROOT "
            "(PowerShell: $env:SEA_DATA_ROOT=\"...\").\n"
            "  To download it: python ingest/fetch.py <record-id> "
            "--dest <that directory>")


def resolve_sensor(sensor, found, root):
    """-> (sensor, None) or (None, message). Picks the only sensor when there
    is exactly one, and lists the real names when the guess was wrong."""
    if sensor is None:
        if len(found) == 1:
            return next(iter(found)), None
        return None, (f"which sensor? {len(found)} found under "
                      f"{os.path.abspath(root)}:\n  "
                      + "\n  ".join(sorted(found))
                      + "\n  Pass one as the first argument.")
    if sensor in found:
        return sensor, None
    near = [s for s in found if s.lower() == sensor.lower()]
    hint = (f"\n  Did you mean: {near[0]}" if near else
            "\n  Available: " + ", ".join(sorted(found)))
    return None, (f"no CSVs for sensor '{sensor}' under "
                  f"{os.path.abspath(root)}." + hint)
