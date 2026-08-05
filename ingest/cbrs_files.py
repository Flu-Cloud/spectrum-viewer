"""
cbrs_files.py: finding and reading CBRS export CSVs.

psd_ingest.py and pfp_ingest.py differ only in the filename pattern they look
for and the shape of the numbers inside. Walking a directory, grouping matches
by sensor and day, explaining an empty result, and turning one CSV into
int8-quantized rows are the same job in both, so they live here once. Each
caller passes its own compiled pattern and its own quantization range.

A matching filename must yield named groups `day`, `sensor` and `stat`.
"""

import os
import sys

import numpy as np


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


def read_quantized(path, csv_connection, nvals, qmin, qmax, unit, what, lead=()):
    """One export CSV -> (epoch_seconds[n], {lead column: values}, uint8[n, nvals]).

    Both deep layers store the same thing in the same way: a timestamp, zero or
    more key columns, then a fixed number of power values per capture, quantized
    to int8 over a per-layer range. PSD has 2250 bins and no key column; PFP has
    560 frame positions keyed by `frequency`. Only those three numbers differ, so
    the reading, the column-count check, the quantization and the out-of-range
    warning are written once here.

    `lead` names the columns between the timestamp and the values, in order.
    """
    res = csv_connection.execute("SELECT * FROM read_csv_auto(?, header=true)", [path])
    cols = [c[0] for c in res.description]
    nlead = 1 + len(lead)
    if len(cols) - nlead != nvals:
        after = " and ".join(["timestamp", *lead]) if lead else "the timestamp"
        raise ValueError(f"expected {nvals} {what} columns after {after}, "
                         f"found {len(cols) - nlead}. This file is not a "
                         f"{nvals}-{'bin' if not lead else 'position'} CBRS "
                         f"{'PSD' if not lead else 'PFP'} export.")
    data = res.fetchnumpy()
    timestamps = data[cols[0]].astype("datetime64[us]").astype("int64") / 1e6  # epoch sec
    keys = {name: np.asarray(data[cols[1 + i]], dtype=np.float64)
            for i, name in enumerate(lead)}
    powers = np.stack([np.asarray(data[c], dtype=np.float64) for c in cols[nlead:]],
                      axis=1)
    quantized = np.clip(np.round((powers - qmin) / (qmax - qmin) * 255.0),
                        0, 255).astype(np.uint8)
    # Values outside the quantization range clip to a flat 0 or 255 instead of
    # failing, so a file in the wrong units (dBm rather than dBm/Hz, say)
    # ingests as a featureless band and looks like a rendering bug later. Name
    # the file now, while it is still obvious which one it was.
    out = int(np.count_nonzero((powers < qmin) | (powers > qmax)))
    if out > powers.size // 100:
        print(f"  WARN {os.path.basename(path)}: {100.0 * out / powers.size:.0f}% of "
              f"values are outside [{qmin}, {qmax}] {unit} and were clipped flat")
    return timestamps, keys, quantized
