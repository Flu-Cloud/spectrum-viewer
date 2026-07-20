"""
fetch.py  -  download NIST Public Data Repository (PDR) datasets for this viewer.

Turns "I have a URL to a NIST dataset" into files on disk, laid out so the
existing ingest scripts run unmodified afterwards. This script only downloads;
it never guesses file types and never runs ingest for you.

    py fetch.py <url-or-record-id> [--dest DIR] [--filter SUBSTRING] [--list] [--flat]

Accepted sources:
    a PDR record id           mds2-3177   /   ark:/88434/mds2-3177
    a PDR record/landing URL  https://data.nist.gov/rmm/records/mds2-3177
                              https://data.nist.gov/od/id/mds2-3177
    a dataset DOI             https://doi.org/10.18434/mds2-3177
    a direct file URL         https://data.nist.gov/od/ds/.../file.tdms

For a record, the PDR JSON manifest is fetched and every downloadable file is
listed (path, size, URL). --filter keeps paths containing the substring
(case-insensitive); the matches are downloaded preserving the record's own
folder structure under --dest (default: ./<record-id>). --flat drops the
folder structure and writes basenames straight into --dest (for e.g.
ingest/csv/, which build_db.py globs non-recursively). Downloads stream to
disk with progress, skip files that are already complete, and resume partial
files via HTTP Range - so Ctrl+C and re-run any time. When the record carries
.sha256 sidecar components, downloaded files are verified against them.

Examples (see README "Bring your own dataset"):
    py ingest/fetch.py mds2-3177 --list
    py ingest/fetch.py mds2-3177 --filter 1.4MHz --dest iqdata/mds2-3177
    py ingest/fetch.py mds2-4214 --filter CBBT-Directional --dest "$env:SEA_DATA_ROOT"

Only public, unauthenticated NIST hosts are allowed (plus NIST's own OAR
download cache on S3, which data.nist.gov redirects to). If a source needs an
invite or login - e.g. the SEA Box share - this script refuses rather than
work around it.
"""

import argparse
import hashlib
import json
import os
import posixpath
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

RMM_RECORD_URL = "https://data.nist.gov/rmm/records/{}"
# data.nist.gov 302s file GETs to NIST's OAR cache bucket; allow exactly that
# host besides *.nist.gov. Redirects anywhere else are refused.
EXTRA_ALLOWED_HOSTS = {"nist-oar-cache.s3.amazonaws.com"}
USER_AGENT = "spectrum-viewer-fetch (github.com/Flu-Cloud/spectrum-viewer)"
CHUNK = 1 << 20            # 1 MiB read chunks
PROGRESS_EVERY = 0.5       # seconds between progress line updates

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


class FetchError(Exception):
    pass


def host_ok(url):
    """True iff url is https on *.nist.gov or the NIST OAR cache host."""
    parts = urllib.parse.urlsplit(url)
    host = (parts.hostname or "").lower()
    if parts.scheme != "https":
        return False
    return (host == "nist.gov" or host.endswith(".nist.gov")
            or host in EXTRA_ALLOWED_HOSTS)


class _NistOnlyRedirects(urllib.request.HTTPRedirectHandler):
    """Follow redirects only onto allowed NIST hosts."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not host_ok(newurl):
            raise FetchError(f"refusing redirect to non-NIST host: {newurl}")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


OPENER = urllib.request.build_opener(_NistOnlyRedirects)


def open_url(url, headers=None, timeout=60):
    if not host_ok(url):
        raise FetchError(f"not an allowed NIST host: {url}\n"
                         "  (only https://*.nist.gov sources are supported; "
                         "invite-only shares like Box are out of scope)")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                               **(headers or {})})
    return OPENER.open(req, timeout=timeout)


def parse_source(src):
    """Classify the CLI argument -> ('record', id) or ('file', url)."""
    s = src.strip()
    if not re.match(r"^[a-z]+:", s, re.I):           # bare token = record id
        return "record", s.split("/")[-1] if "ark:" in s else s
    if s.lower().startswith("doi:"):
        return "record", s.split("/")[-1]
    parts = urllib.parse.urlsplit(s)
    host = (parts.hostname or "").lower()
    path = urllib.parse.unquote(parts.path)
    if host == "doi.org":                            # https://doi.org/10.18434/<id>
        return "record", path.rstrip("/").split("/")[-1]
    m = re.search(r"/(?:rmm/records|od/id)/(.+?)/?$", path)
    if m:                                            # record/landing URL
        return "record", m.group(1).split("/")[-1]
    if host.endswith(".nist.gov") and path not in ("", "/"):
        return "file", s                             # direct file URL
    raise FetchError(f"can't interpret source: {src}\n"
                     "  expected a PDR record id/URL/DOI or a direct "
                     "https://*.nist.gov file URL")


def fetch_record(rid):
    """PDR record id -> manifest dict (handles the RMM ResultData envelope)."""
    url = RMM_RECORD_URL.format(urllib.parse.quote(rid))
    try:
        with open_url(url) as r:
            doc = json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise FetchError(
                f"PDR has no record '{rid}' (HTTP 404). If this id comes from "
                "a NIST page, the record may not be published yet.") from e
        raise
    if isinstance(doc, dict) and "ResultData" in doc:      # RMM envelope
        if not doc.get("ResultCount") or not doc["ResultData"]:
            raise FetchError(f"PDR record '{rid}' not found (empty ResultData)")
        doc = doc["ResultData"][0]
    if not isinstance(doc, dict) or not isinstance(doc.get("components"), list):
        raise FetchError(f"unexpected manifest shape for '{rid}' "
                         "(no components list)")
    return doc


def safe_relpath(fp):
    """Validate a manifest filepath before using it on disk."""
    if not fp or fp.startswith(("/", "\\")):
        return False
    return all(seg not in ("", ".", "..") and "\\" not in seg and ":" not in seg
               for seg in fp.split("/"))


def record_files(rec):
    """-> (data_files, checksums) from a record's components.

    data_files: list of dicts {filepath, url, size}; checksums: filepath of the
    checksummed file -> sidecar downloadURL. Components without a downloadURL
    (subcollections, access pages) are ignored; components with a bad host or
    unsafe filepath are dropped with a warning.
    """
    files, checksums, dropped = [], {}, 0
    for c in rec["components"]:
        url = c.get("downloadURL")
        if not url:
            continue
        types = c.get("@type") or []
        fp = c.get("filepath") or posixpath.basename(
            urllib.parse.unquote(urllib.parse.urlsplit(url).path))
        if not host_ok(url) or not safe_relpath(fp):
            print(f"  WARNING: skipping component with unexpected "
                  f"host/path: {fp!r} <- {url}")
            dropped += 1
            continue
        size = c.get("size")
        size = int(size) if isinstance(size, (int, float)) else None
        if "nrdp:ChecksumFile" in types or fp.endswith(".sha256"):
            checksums[fp[:-len(".sha256")] if fp.endswith(".sha256") else fp] = url
        else:
            files.append({"filepath": fp, "url": url, "size": size})
    if dropped:
        print(f"  ({dropped} component(s) dropped - not on an allowed NIST host)")
    return files, checksums


def fmt_size(n):
    if n is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024.0


def fmt_eta(sec):
    if sec is None or sec != sec or sec < 0 or sec > 99 * 3600:
        return "--:--"
    sec = int(sec)
    if sec >= 3600:
        return f"{sec // 3600}:{sec % 3600 // 60:02d}:{sec % 60:02d}"
    return f"{sec // 60}:{sec % 60:02d}"


def head_size(url):
    try:
        req = urllib.request.Request(url, method="HEAD",
                                     headers={"User-Agent": USER_AGENT})
        with OPENER.open(req, timeout=30) as r:
            cl = r.headers.get("Content-Length")
            return int(cl) if cl else None
    except Exception:
        return None


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_checksum(path, sidecar_url):
    """True if file matches its .sha256 sidecar; deletes the file on mismatch."""
    with open_url(sidecar_url, timeout=30) as r:
        text = r.read(4096).decode("ascii", "replace")
    m = re.search(r"\b[0-9a-fA-F]{64}\b", text)
    if not m:
        print("      checksum sidecar unreadable - skipping verification")
        return True
    if sha256_file(path).lower() == m.group(0).lower():
        return True
    os.remove(path)   # so a re-run re-downloads instead of skip-as-complete
    print(f"      CHECKSUM MISMATCH - deleted {path}; re-run to re-download")
    return False


def download(url, path, expected, label):
    """Stream url -> path. Skip if complete, Range-resume if partial.

    Returns 'done' | 'skipped' | 'failed'.
    """
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    have = os.path.getsize(path) if os.path.exists(path) else 0
    if expected is not None and have == expected:
        print(f"{label} already complete ({fmt_size(expected)}) - skipped")
        return "skipped"
    if expected is not None and have > expected:
        print(f"{label} local file larger than expected - restarting")
        have = 0

    headers = {"Range": f"bytes={have}-"} if have else {}
    try:
        resp = open_url(url, headers=headers)
    except urllib.error.HTTPError as e:
        if e.code == 416 and have:      # nothing left to serve
            print(f"{label} already complete ({fmt_size(have)}) - skipped")
            return "skipped"
        print(f"{label} FAILED: HTTP {e.code}")
        return "failed"

    with resp:
        if have and resp.status == 206:
            mode = "ab"
        else:                            # fresh download, or Range ignored
            mode, have = "wb", 0
        cl = resp.headers.get("Content-Length")
        total = expected if expected is not None else \
            (have + int(cl)) if cl else None
        done = have
        t0 = last = time.time()
        base = have
        with open(path, mode) as out:
            while True:
                chunk = resp.read(CHUNK)
                if not chunk:
                    break
                out.write(chunk)
                done += len(chunk)
                now = time.time()
                if now - last >= PROGRESS_EVERY:
                    last = now
                    rate = (done - base) / max(now - t0, 1e-9)
                    eta = (total - done) / rate if total and rate else None
                    pct = f"{done / total * 100:3.0f}%" if total else "  ?%"
                    print(f"\r{label} {fmt_size(done):>9} / {fmt_size(total):>9}"
                          f"  {pct}  {fmt_size(rate)}/s  ETA {fmt_eta(eta)}   ",
                          end="")
    rate = (done - base) / max(time.time() - t0, 1e-9)
    print(f"\r{label} {fmt_size(done):>9} downloaded "
          f"({fmt_size(rate)}/s)" + " " * 24)
    if expected is not None and done != expected:
        print(f"{label} INCOMPLETE ({fmt_size(done)} of {fmt_size(expected)}) "
              "- re-run to resume")
        return "failed"
    return "done"


def main():
    ap = argparse.ArgumentParser(
        description="Download a NIST PDR record (or a direct NIST file URL) "
                    "into the layout the ingest scripts expect. See the "
                    "module docstring / README 'Bring your own dataset'.")
    ap.add_argument("source", help="PDR record id/URL/DOI, or direct file URL")
    ap.add_argument("--dest", default=None,
                    help="target directory (default: ./<record-id>, or . for "
                         "a direct file URL)")
    ap.add_argument("--filter", default=None, metavar="SUBSTRING",
                    help="only files whose path contains SUBSTRING "
                         "(case-insensitive), e.g. CBBT-Directional or 1.4MHz")
    ap.add_argument("--list", action="store_true",
                    help="list matching files (path, size, URL) and exit")
    ap.add_argument("--flat", action="store_true",
                    help="ignore record folder structure; write basenames "
                         "directly into --dest (for ingest/csv)")
    args = ap.parse_args()

    kind, val = parse_source(args.source)

    if kind == "file":
        if args.filter or args.list or args.flat:
            sys.exit("--filter/--list/--flat only apply to PDR records")
        name = posixpath.basename(
            urllib.parse.unquote(urllib.parse.urlsplit(val).path))
        if not name or not safe_relpath(name):
            sys.exit(f"can't derive a safe filename from {val}")
        path = os.path.join(args.dest or ".", name)
        status = download(val, path, head_size(val), f"  {name}")
        sys.exit(0 if status != "failed" else 1)

    rec = fetch_record(val)
    title = (rec.get("title") or "").strip()
    print(f"PDR record {val}: {title}")
    files, checksums = record_files(rec)
    if args.filter:
        needle = args.filter.lower()
        files = [f for f in files if needle in f["filepath"].lower()]
    if not files:
        sys.exit(f"no downloadable files"
                 + (f" matching '{args.filter}'" if args.filter else "")
                 + " in this record")
    total = sum(f["size"] or 0 for f in files)
    known = all(f["size"] is not None for f in files)
    print(f"{len(files)} file(s), {fmt_size(total)}{'' if known else '+'} total"
          + (f" (filter: {args.filter})" if args.filter else "")
          + (f"; {len(checksums)} .sha256 sidecar(s) for verification"
             if checksums else ""))

    if args.list:
        for f in sorted(files, key=lambda f: f["filepath"]):
            print(f"  {fmt_size(f['size']):>10}  {f['filepath']}")
            print(f"              {f['url']}")
        return

    dest = args.dest or val
    if args.flat:
        names = [posixpath.basename(f["filepath"]) for f in files]
        if len(set(names)) != len(names):
            sys.exit("--flat would overwrite files that share a basename; "
                     "drop --flat or narrow --filter")
    print(f"downloading into {os.path.abspath(dest)}"
          + (" (flattened)" if args.flat else "") + "\n")

    counts = {"done": 0, "skipped": 0, "failed": 0}
    try:
        for i, f in enumerate(sorted(files, key=lambda f: f["filepath"]), 1):
            rel = posixpath.basename(f["filepath"]) if args.flat else f["filepath"]
            path = os.path.join(dest, *rel.split("/"))
            label = f"  [{i}/{len(files)}] {rel}"
            status = download(f["url"], path, f["size"], label)
            if status == "done" and f["filepath"] in checksums:
                if not verify_checksum(path, checksums[f["filepath"]]):
                    status = "failed"
            counts[status] += 1
    except KeyboardInterrupt:
        print("\n\ninterrupted - partial files kept; "
              "re-run the same command to resume")
        sys.exit(130)

    print(f"\ndone: {counts['done']} downloaded, {counts['skipped']} already "
          f"complete, {counts['failed']} failed"
          + ("" if not counts["failed"] else " - re-run to retry/resume"))
    print("next: run the matching ingest script "
          "(build_db.py / psd_ingest.py / pfp_ingest.py / iq_ingest.py)")
    sys.exit(1 if counts["failed"] else 0)


if __name__ == "__main__":
    try:
        main()
    except FetchError as e:
        sys.exit(f"fetch.py: {e}")
