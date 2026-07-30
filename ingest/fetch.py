#!/usr/bin/env python3
"""
fetch.py: download a dataset for this viewer, from NIST PDR or anywhere else.

Turns "I have a URL to a dataset" into files on disk, laid out so the ingest
scripts run afterwards. This script downloads and then tells you which ingest
command to run next; it does not run it for you.

    py fetch.py <url-or-record-id> [--dest DIR] [--filter SUBSTRING]
                [--list | --long | --tree] [--flat]

Accepted sources:
    a PDR record id           mds2-3177
    an ark id                 ark:/88434/mds2-3177
    a PDR record/landing URL  https://data.nist.gov/rmm/records/mds2-3177
                              https://data.nist.gov/od/id/mds2-3177
    a dataset DOI             https://doi.org/10.18434/mds2-3177  or  doi:10.18434/mds2-3177
    any direct file URL       https://example.org/path/capture.sigmf-data

For a record, the PDR JSON manifest is fetched and every downloadable file is
listed. --list summarises the record by folder, --tree shows just the folder
structure, --long prints every file with its URL. --filter keeps paths
containing a substring (case-insensitive); matches are downloaded preserving
the record's folder structure under --dest (default: ./<record-id>). --flat
drops that structure and writes basenames straight into --dest (for e.g.
ingest/csv/, which build_db.py reads).

Downloads stream to disk with progress, skip files that are already complete,
resume partial files via HTTP Range, and retry transient network errors, so
Ctrl+C and re-run any time. When the record carries .sha256 sidecar
components, downloaded files are verified against them.

Host policy: any https:// source is allowed. Downloading from a host outside
nist.gov prints one warning line and continues. Use --nist-only for the strict
behaviour, --allow-host HOST to whitelist a host under it, and --allow-http to
permit unencrypted http:// (off by default because it is not authenticated).

Getting past HTTP 403: data.nist.gov sits behind an edge WAF that rejects
clients it reads as bots, so requests here carry a browser's header set and a
403 escalates through several of them rather than failing outright. A record id
is also looked up at more than one PDR endpoint, because one being blocked or
moved should not sink the fetch. If a host still refuses everything, --user-agent
(or ATLAS_USER_AGENT) sends a string of your choosing, and the failure message
names the workaround that always works: download in a browser, then run
`python atlas.py get <the folder>`.

Examples (see README "Bring your own dataset"):
    py ingest/fetch.py mds2-3177 --list
    py ingest/fetch.py mds2-3177 --filter 1.4MHz --dest iqdata/mds2-3177
    py ingest/fetch.py mds2-4214 --filter CBBT-Directional --dest SEA-DATA
"""

import argparse
import hashlib
import http.client
import json
import os
import posixpath
import re
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# Where a record id is looked up. data.nist.gov publishes the same record
# through more than one endpoint and they do not fail together: the RMM search
# API can be down, moved or blocked by the edge while the /od/id resolver still
# answers. Each is tried in turn before giving up, which is the difference
# between "HTTP 403, nothing downloaded" and a working fetch.
# ATLAS_PDR_BASE replaces the whole list with one template, which is how the
# offline tests point this at a local fixture:
#     ATLAS_PDR_BASE="http://127.0.0.1:8000/rmm/records/{}"
RECORD_ENDPOINTS = (
    "https://data.nist.gov/rmm/records/{}",
    "https://data.nist.gov/rmm/records?@id=ark:/88434/{}",
    "https://data.nist.gov/od/id/{}",
)
# Hosts that never trigger the "leaving NIST" warning. data.nist.gov 302s file
# GETs to NIST's OAR cache bucket, so that bucket counts as NIST too.
TRUSTED_SUFFIXES = (".nist.gov",)
TRUSTED_HOSTS = {"nist.gov", "nist-oar-cache.s3.amazonaws.com"}
CHUNK = 1 << 20            # 1 MiB read chunks
PROGRESS_EVERY = 0.5       # seconds between progress line updates
RETRY_CODES = {408, 425, 429, 500, 502, 503, 504}
# Codes that mean "the edge rejected this request", not "the network failed".
# Waiting and retrying identically cannot fix these; sending a different header
# set can, so they drive the profile ladder below instead of a backoff.
REJECT_CODES = {401, 403, 406}

# data.nist.gov, like most .gov hosts, sits behind an edge WAF that answers 403
# to clients it classifies as bots -- and a bare "atlas-fetch/1.0" User-Agent is
# classified as one, which is why a download that works in a browser failed here
# with HTTP 403 and ingested nothing. Sending the header set a browser sends
# fixes it. This is not pretending to be a person: the tool requests exactly the
# public files a person clicking the same record would, one at a time, and
# --user-agent / ATLAS_USER_AGENT overrides it for hosts that want something
# specific (a named crawler, or a campus proxy that allowlists one string).
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
PROJECT_UA = "atlas-fetch (github.com/jimmylu7/ATLAS)"

# Tried in order, advancing only when the edge rejects the request outright.
# Profile 0 works for effectively every host; the rest exist because a single
# 403 used to end the whole download.
HEADER_PROFILES = (
    # 0: a plain browser GET.
    {"User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9"},
    # 1: the same plus the navigation headers a browser sends when the click
    #    came from the file's own site. Some WAF rules want a same-origin
    #    Referer before they will serve a file. The Referer is built from the
    #    target's own origin, so nothing leaks about where the run came from.
    {"User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9",
     "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors",
     "Sec-Fetch-Site": "same-origin", "_same_origin_referer": "1"},
    # 2: identify honestly as this tool. Last because the hosts that reward it
    #    are the minority, but it is the right thing to send to one that keeps
    #    an allowlist of named clients.
    {"User-Agent": PROJECT_UA},
)


def build_headers(profile, url, extra=None):
    """The headers for one attempt: a profile, plus whatever the caller adds.

    Accept-Encoding is pinned to identity on purpose. Every byte count in this
    file -- resume offsets, the size check against the manifest, the progress
    line -- counts body bytes on the wire, so a transparently gzipped response
    would make all of them wrong.
    """
    h = {k: v for k, v in HEADER_PROFILES[profile].items()
         if not k.startswith("_")}
    if HEADER_PROFILES[profile].get("_same_origin_referer"):
        parts = urllib.parse.urlsplit(url)
        h["Referer"] = f"{parts.scheme}://{parts.netloc}/"
    h.setdefault("Accept", "*/*")
    h["Accept-Encoding"] = "identity"
    override = os.environ.get("ATLAS_USER_AGENT")
    if override:
        h["User-Agent"] = override
    h.update(extra or {})
    return h


def record_endpoints():
    override = os.environ.get("ATLAS_PDR_BASE")
    return (override,) if override else RECORD_ENDPOINTS

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


class FetchError(Exception):
    pass


# ---- host policy ------------------------------------------------------

def is_trusted_host(host):
    host = (host or "").lower()
    return host in TRUSTED_HOSTS or host.endswith(TRUSTED_SUFFIXES)


class Policy:
    """Decides which URLs may be opened, and which ones deserve a warning.

    Default: any https host. --nist-only restores the old allowlist, which is
    still useful when you want a run to provably touch nothing but NIST.
    """

    def __init__(self, nist_only=False, allow_http=False, extra_hosts=()):
        self.nist_only = nist_only
        self.allow_http = allow_http
        self.extra = {h.lower() for h in extra_hosts}
        self._warned = set()

    def refusal(self, url):
        """None if the URL may be opened, else the reason it may not be."""
        parts = urllib.parse.urlsplit(url)
        host = (parts.hostname or "").lower()
        if not host:
            return f"no host in URL: {url}"
        if parts.scheme == "http" and not self.allow_http:
            return (f"{url}\n  http:// is unencrypted and unauthenticated. Use "
                    "https:// if the server supports it, or pass --allow-http.")
        if parts.scheme not in ("http", "https"):
            return f"unsupported URL scheme {parts.scheme!r}: {url}"
        if self.nist_only and not (is_trusted_host(host) or host in self.extra):
            return (f"{url}\n  --nist-only is set and {host} is not a NIST host. "
                    f"Drop --nist-only, or pass --allow-host {host}.")
        return None

    def allows(self, url):
        return self.refusal(url) is None

    def note(self, url):
        """Warn once per host when a download leaves NIST."""
        host = (urllib.parse.urlsplit(url).hostname or "").lower()
        if not host or host in self._warned:
            return
        if is_trusted_host(host) or host in self.extra:
            return
        self._warned.add(host)
        print(f"  note: {host} is not a NIST host. Downloading anyway "
              "(pass --nist-only to refuse).")


POLICY = Policy()


class _PolicyRedirects(urllib.request.HTTPRedirectHandler):
    """Apply the same policy to redirect targets as to the original URL."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        why = POLICY.refusal(newurl)
        if why:
            raise FetchError(f"refusing redirect to {why}")
        POLICY.note(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


OPENER = urllib.request.build_opener(_PolicyRedirects)


# ---- network plumbing -------------------------------------------------

TRANSIENT = (urllib.error.URLError, socket.timeout, ConnectionError,
             http.client.IncompleteRead, http.client.RemoteDisconnected, OSError)


def _proxy_hint():
    proxy = (os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
             or os.environ.get("ALL_PROXY") or os.environ.get("all_proxy"))
    if proxy:
        return (f"\n  A proxy is configured (HTTPS_PROXY={proxy}). If it blocks "
                "this host, ask whoever runs it to allow it, or unset the "
                "variable on a network that does not need it.")
    return ("\n  If you are on a corporate or campus network, you may need to "
            "set HTTPS_PROXY before running this.")


def _tls_error(exc):
    """The ssl.SSLError inside exc, if this failure is a TLS one at all.

    urlopen wraps it in a URLError, so the interesting exception is one level
    down from what the caller catches.
    """
    for e in (getattr(exc, "reason", None), exc):
        if isinstance(e, ssl.SSLError):
            return e
    return None


def _cert_hint(host, err):
    """Which of the three TLS failures this is, and the fix for that one.

    Verification failures are not interchangeable and the wrong guess sends
    people the wrong way. An expired certificate is nearly always the local
    trust store rather than the server: a public host's certificate is renewed
    long before it lapses, so a client alone in seeing it expired is a client
    carrying a CA bundle old enough that the issuer it pins has itself run out.
    """
    # Which of these carries the detail varies by Python version and by how
    # the error was wrapped: .reason is the bare code, .verify_message the
    # human half, str() usually both. Read all three rather than pick one.
    text = " ".join(str(v) for v in (err, getattr(err, "reason", ""),
                                     getattr(err, "verify_message", "")) if v)
    if "CERTIFICATE_VERIFY_FAILED" not in text.upper():
        return ("  The handshake itself failed, before any certificate check."
                + _proxy_hint())
    low = text.lower()
    if "expired" in low:
        return (
            f"  Almost certainly this machine's CA bundle, not {host}: a public\n"
            "  host renews well before expiry, so a client alone in seeing it "
            "expired\n"
            "  is usually carrying roots old enough to have lapsed themselves.\n"
            "  Update them:\n"
            "      python -m pip install --upgrade certifi\n"
            "  On macOS also run the bundled "
            "\"Install Certificates.command\" for your\n"
            "  Python. Then check what the host is actually serving:\n"
            f"      python -c \"import ssl,socket;"
            f"print(ssl.create_default_context().wrap_socket("
            f"socket.create_connection(('{host}',443)),"
            f"server_hostname='{host}').getpeercert()['notAfter'])\"\n"
            "  If that prints a future date, the bundle was the problem and the "
            "download\n  will now work." + _proxy_hint())
    if "self signed" in low or "self-signed" in low:
        return ("  A proxy is intercepting HTTPS with its own certificate. "
                "Point\n  SSL_CERT_FILE at your organisation's CA bundle rather "
                "than disabling\n  verification." + _proxy_hint())
    if "hostname mismatch" in low or "doesn't match" in low:
        return (f"  The certificate served is not valid for {host}. Check the "
                "URL, and\n  whether something is redirecting the host."
                + _proxy_hint())
    return ("  Point SSL_CERT_FILE at the CA bundle this network needs rather "
            "than\n  disabling verification." + _proxy_hint())


def net_error(url, exc):
    """Turn any network exception into one sentence a human can act on."""
    host = urllib.parse.urlsplit(url).hostname or url
    if isinstance(exc, urllib.error.HTTPError):
        return FetchError(f"{host} returned HTTP {exc.code} {exc.reason} for {url}")
    cause = getattr(exc, "reason", exc)
    tls = _tls_error(exc)
    if tls is not None:
        return FetchError(f"TLS verification against {host} failed: {cause}\n"
                          + _cert_hint(host, tls))
    if isinstance(cause, socket.gaierror):
        return FetchError(f"cannot resolve {host}: {cause}. Check the URL "
                          "spelling and that you are online.")
    return FetchError(f"cannot reach {host}: {cause}" + _proxy_hint())


def _is_transient(exc):
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in RETRY_CODES
    # A rejected certificate is a decision, not a hiccup: the same bundle will
    # reject it again every time. ssl.SSLError inherits OSError, so without
    # this it matches TRANSIENT below and burns the whole retry ladder --
    # nine handshakes and ~20s -- before reporting what was already known on
    # the first. Checked ahead of TRANSIENT for exactly that reason.
    if _tls_error(exc) is not None:
        return False
    return isinstance(exc, TRANSIENT)


def blocked_hint(url, code):
    """What to actually do about a 401/403. Printed once, next to the failure.

    A rejection here is nearly always one of three things, and the user can fix
    all three -- but only if told which they are looking at.
    """
    host = urllib.parse.urlsplit(url).hostname or url
    return (
        f"  HTTP {code} means {host} refused the request itself, not that the "
        "file is missing.\n"
        "  Three things cause this, in order of likelihood:\n"
        f"    1. {host} is behind bot protection that rejects non-browser "
        "clients.\n"
        "       Every browser header set this tool knows was already tried. A "
        "different\n"
        "       one may work: --user-agent \"<string>\" (or set "
        "ATLAS_USER_AGENT).\n"
        "    2. A proxy or campus/corporate firewall is blocking the host."
        + _proxy_hint().replace("\n  ", "\n       ") + "\n"
        "    3. The record is embargoed or withdrawn, so nothing will fetch it.\n"
        "  This never has to block you. Download the files in a browser, then "
        "point\n"
        "  ATLAS at the folder -- the ingest is identical either way:\n"
        "      python atlas.py get \"<the folder you saved them in>\"")


def open_url(url, headers=None, timeout=60, retries=3):
    """Open url under the current policy.

    Two independent retry ladders run here, because two different things go
    wrong. A dropped connection or a 5xx is transient: the same request is
    retried with a growing backoff. A 401/403/406 is not -- the edge looked at
    the request and rejected it, so waiting changes nothing and the next attempt
    sends a different header profile instead. That second ladder is what makes
    data.nist.gov work: its WAF 403s clients it reads as bots, and the profile
    that gets through is not the first one for every host.
    """
    why = POLICY.refusal(url)
    if why:
        raise FetchError(f"not allowed: {why}")
    POLICY.note(url)
    rejected = None
    for profile in range(len(HEADER_PROFILES)):
        req = urllib.request.Request(
            url, headers=build_headers(profile, url, headers))
        delay = 1.0
        for attempt in range(retries + 1):
            try:
                return OPENER.open(req, timeout=timeout)
            except FetchError:
                raise
            except urllib.error.HTTPError as e:
                if e.code in REJECT_CODES:
                    rejected = e
                    break                            # next header profile
                if attempt >= retries or e.code not in RETRY_CODES:
                    raise                            # callers inspect .code
                print(f"  HTTP {e.code} {e.reason}; retry {attempt + 1}/"
                      f"{retries} in {delay:.0f}s")
            except Exception as e:                   # noqa: BLE001
                if attempt >= retries or not _is_transient(e):
                    raise net_error(url, e) from e
                print(f"  transient error ({e}); retry {attempt + 1}/{retries} "
                      f"in {delay:.0f}s")
            time.sleep(delay)
            delay *= 2
        if rejected is not None and profile + 1 < len(HEADER_PROFILES):
            print(f"  HTTP {rejected.code} from "
                  f"{urllib.parse.urlsplit(url).hostname}; retrying as a "
                  "different client")
    raise rejected


# ---- source parsing ---------------------------------------------------

# Characters that ride along when a URL is pasted out of a browser, an email,
# a chat message or a markdown link. Stripping them is the difference between
# a working paste and a confusing 404 on a record id like 'mds2-3177>'.
_PASTE_JUNK = " \t\r\n<>\"'`,;"


def clean_source(src):
    s = src.strip().strip(_PASTE_JUNK)
    m = re.match(r"^\[[^\]]*\]\((.+)\)$", s)         # a whole markdown link
    if m:
        s = m.group(1).strip(_PASTE_JUNK)
    elif s.endswith(")") and "(" not in s:           # just its trailing paren
        s = s[:-1].strip(_PASTE_JUNK)
    return s


def _last_segment(s):
    return s.rstrip("/").split("/")[-1]


def parse_source(src):
    """Classify the CLI argument -> ('record', id) or ('file', url).

    Accepts every shape a NIST PDR page hands you: the landing URL, the ark
    form, the RMM manifest URL and its ?@id= query form, a DOI, the record-level
    /od/ds/ endpoint, a bare id, and any direct file URL.
    """
    s = clean_source(src)
    if not s:
        raise FetchError("empty source")
    low = s.lower()
    if low.startswith(("ark:", "doi:")):             # ark:/88434/mds2-3177
        return "record", _last_segment(s)
    if not re.match(r"^[a-z][a-z0-9+.-]*://", s, re.I):
        # No scheme. Either a bare id, or a pasted URL missing its https://.
        return "record", _last_segment(s.split("?")[0].split("#")[0])
    parts = urllib.parse.urlsplit(s)
    host = (parts.hostname or "").lower()
    path = urllib.parse.unquote(parts.path)
    if host in ("doi.org", "dx.doi.org", "n2t.net"):
        return "record", _last_segment(path)
    m = re.search(r"/(?:rmm/records|od/id)/(.+?)/?$", path)
    if m:                                            # record/landing URL
        return "record", _last_segment(m.group(1))
    if re.search(r"/(?:rmm/records|od/id)/?$", path):
        # the RMM query form: /rmm/records?@id=ark:/88434/mds2-3177
        qs = urllib.parse.parse_qs(parts.query)
        for key in ("@id", "id", "recordId"):
            if qs.get(key):
                return "record", _last_segment(urllib.parse.unquote(qs[key][0]))
    m = re.search(r"/od/ds/(.+?)/?$", path)
    if m and "." not in posixpath.basename(m.group(1).rstrip("/")):
        # /od/ds/<id> with no filename is the record, not a file
        return "record", _last_segment(m.group(1))
    if path in ("", "/"):
        raise FetchError(f"can't interpret source: {src}\n"
                         "  that URL has no path. Expected a PDR record "
                         "id/URL/DOI, or a direct file URL.")
    return "file", s


def unwrap_record(doc):
    """A manifest response -> the record dict, or None if there isn't one.

    The endpoints disagree about packaging: the RMM search API wraps hits in a
    ResultData envelope, the /od/id resolver returns the NERDm record straight.
    Both end at the same thing, a dict with a components list.
    """
    if isinstance(doc, dict) and "ResultData" in doc:      # RMM envelope
        if not doc.get("ResultCount") or not doc["ResultData"]:
            return None
        doc = doc["ResultData"][0]
    if isinstance(doc, dict) and isinstance(doc.get("components"), list):
        return doc
    return None


def record_error(rid, problems):
    """One actionable message from every endpoint that was tried and failed."""
    codes = {code for _url, _why, code in problems if code}
    tried = "\n".join(f"    {url}\n      {why}" for url, why, _c in problems)
    if codes and codes <= {404}:
        return FetchError(
            f"the repository has no record '{rid}' (HTTP 404 from every "
            "endpoint). If this id comes from a NIST page, the record may not "
            f"be published yet.\n  Tried:\n{tried}")
    head = (f"could not read the manifest for record '{rid}'.\n"
            f"  Tried {len(problems)} endpoint(s):\n{tried}")
    blocking = sorted(c for c in codes if c in REJECT_CODES)
    if blocking:
        first = next(u for u, _w, c in problems if c in REJECT_CODES)
        return FetchError(head + "\n" + blocked_hint(first, blocking[0]))
    return FetchError(head)


def fetch_record(rid, timeout=60, retries=3):
    """PDR record id -> manifest dict, trying each known endpoint in turn.

    One endpoint being blocked, moved or down no longer sinks the whole fetch;
    only failing at all of them does.
    """
    problems = []
    for tmpl in record_endpoints():
        url = tmpl.format(urllib.parse.quote(rid))
        try:
            with open_url(url, headers={"Accept": "application/json"},
                          timeout=timeout, retries=retries) as r:
                doc = json.load(r)
        except urllib.error.HTTPError as e:
            problems.append((url, f"HTTP {e.code} {e.reason}", e.code))
            continue
        except FetchError as e:
            problems.append((url, str(e).replace("\n", "\n      "), None))
            continue
        except json.JSONDecodeError as e:
            problems.append((url, f"did not return JSON ({e}); that endpoint "
                                  "served a page, not a manifest", None))
            continue
        rec = unwrap_record(doc)
        if rec is not None:
            return rec
        problems.append((url, "answered, but with no record in it", None))
    raise record_error(rid, problems)


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
    (subcollections, access pages) are ignored. Components the host policy
    refuses, or with an unsafe filepath, are dropped with a reason.
    """
    files, checksums, refused, unsafe = [], {}, [], []
    for c in rec["components"]:
        url = c.get("downloadURL")
        if not url:
            continue
        types = c.get("@type") or []
        fp = c.get("filepath") or posixpath.basename(
            urllib.parse.unquote(urllib.parse.urlsplit(url).path))
        if not safe_relpath(fp):
            unsafe.append(fp)
            continue
        if not POLICY.allows(url):
            refused.append(url)
            continue
        size = c.get("size")
        if isinstance(size, str) and size.strip().isdigit():
            size = int(size)                # some records quote the number
        size = int(size) if isinstance(size, (int, float)) else None
        if "nrdp:ChecksumFile" in types or fp.endswith(".sha256"):
            checksums[fp[:-len(".sha256")] if fp.endswith(".sha256") else fp] = url
        else:
            files.append({"filepath": fp, "url": url, "size": size})
    if unsafe:
        print(f"  WARNING: {len(unsafe)} component(s) dropped for an unsafe "
              f"path, e.g. {unsafe[0]!r}")
    if refused:
        hosts = sorted({urllib.parse.urlsplit(u).hostname or "?" for u in refused})
        print(f"  WARNING: {len(refused)} component(s) dropped by the host "
              f"policy (hosts: {', '.join(hosts)}).")
        print("           Drop --nist-only, or pass "
              + " ".join(f"--allow-host {h}" for h in hosts))
    return files, checksums


# ---- formatting -------------------------------------------------------

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


def capture_unit(fp):
    """The unit of data a file belongs to, for picking whole ones.

    A SigMF capture is a .sigmf-meta describing a .sigmf-data and neither half
    is usable without the other, so they have to be taken or left together.
    Without this, "the 3 smallest files" of a record full of captures is 3 tiny
    metadata files and no signal at all -- a download that succeeds and then
    ingests nothing.
    """
    low = fp.lower()
    for ext in (".sigmf-meta", ".sigmf-data"):
        if low.endswith(ext):
            return fp[:-len(ext)]
    return fp


def smallest_units(files, n):
    """The n smallest units of data, keeping multi-file captures whole."""
    units = {}
    for f in files:
        units.setdefault(capture_unit(f["filepath"]), []).append(f)
    ordered = sorted(units.values(),
                     key=lambda g: (any(x["size"] is None for x in g),
                                    sum(x["size"] or 0 for x in g),
                                    g[0]["filepath"]))
    return [f for g in ordered[:n] for f in g], len(units)


def group_by_dir(files):
    """-> ordered {directory: [file, ...]} keyed by posix dirname ('.' = root)."""
    out = {}
    for f in sorted(files, key=lambda f: f["filepath"]):
        out.setdefault(posixpath.dirname(f["filepath"]) or ".", []).append(f)
    return out


def print_summary(files):
    """One line per folder: count and total size. Readable for 900 files."""
    groups = group_by_dir(files)
    print(f"\n{len(groups)} folder(s):\n")
    for d, fs in groups.items():
        total = sum(f["size"] or 0 for f in fs)
        print(f"  {len(fs):>5} file(s)  {fmt_size(total):>10}  {d}/")
    print("\nuse --long for every file and its URL, --tree for structure only,")
    print("and --filter SUBSTRING to narrow what gets downloaded.")


def print_tree(files):
    """Directory structure only, indented, with per-directory file counts."""
    counts = {}
    for f in files:
        d = posixpath.dirname(f["filepath"])
        counts[d] = counts.get(d, 0) + 1
        while d:
            d = posixpath.dirname(d)
            counts.setdefault(d, 0)
    print()
    for d in sorted(counts):
        depth = 0 if not d else d.count("/") + 1
        name = posixpath.basename(d) if d else "."
        n = counts[d]
        print(f"  {'  ' * depth}{name}/" + (f"   [{n} file(s)]" if n else ""))


def print_long(files):
    for f in sorted(files, key=lambda f: f["filepath"]):
        print(f"  {fmt_size(f['size']):>10}  {f['filepath']}")
        print(f"              {f['url']}")


def next_step(paths, dest):
    """Suggest the ingest command that matches what was actually downloaded."""
    names = [os.path.basename(p).lower() for p in paths]
    if any(n.endswith((".sigmf-meta", ".tdms", ".npy")) for n in names):
        return (f"python ingest/iq_ingest.py {dest} "
                f"--dataset {os.path.basename(os.path.normpath(dest))}")
    if any(re.search(r"^pfp_.*\.csv$", n) for n in names):
        return f"python ingest/pfp_ingest.py --root {dest}"
    if any(re.search(r"_max\.csv$|_mean\.csv$|_median\.csv$", n) for n in names):
        return f"python ingest/psd_ingest.py --root {dest}"
    if any(n.endswith(".csv") for n in names):
        return (f"python ingest/build_db.py --csv-dir {dest}   "
                "# if these are Summaries CSVs")
    return None


# ---- downloading ------------------------------------------------------

def head_size(url, timeout=30):
    """Content-Length for a direct file URL, or None if it can't be had.

    Only ever an optimisation: it turns on the resume and skip-if-complete
    paths. A host that refuses HEAD (S3 pre-signed URLs answer 403) just means
    the download runs without a known total.
    """
    for profile in range(len(HEADER_PROFILES)):
        try:
            if not POLICY.allows(url):
                return None
            req = urllib.request.Request(
                url, method="HEAD", headers=build_headers(profile, url))
            with OPENER.open(req, timeout=timeout) as r:
                cl = r.headers.get("Content-Length")
                return int(cl) if cl else None
        except urllib.error.HTTPError as e:
            if e.code in REJECT_CODES:
                continue                     # try the next header profile
            return None
        except Exception:                                   # noqa: BLE001
            return None
    return None


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_checksum(path, sidecar_url):
    """True if file matches its .sha256 sidecar; deletes the file on mismatch."""
    try:
        with open_url(sidecar_url, timeout=30) as r:
            text = r.read(4096).decode("ascii", "replace")
    except (FetchError, urllib.error.HTTPError) as e:
        print(f"      could not fetch checksum sidecar ({e}); skipping verification")
        return True
    m = re.search(r"\b[0-9a-fA-F]{64}\b", text)
    if not m:
        print("      checksum sidecar unreadable; skipping verification")
        return True
    if sha256_file(path).lower() == m.group(0).lower():
        return True
    os.remove(path)   # so a re-run re-downloads instead of skip-as-complete
    print(f"      CHECKSUM MISMATCH: deleted {path}; re-run to re-download")
    return False


def download(url, path, expected, label, retries=3, timeout=60):
    """Stream url -> path. Skip if complete, Range-resume if partial.

    Returns 'done' | 'skipped' | 'failed'.
    """
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    have = os.path.getsize(path) if os.path.exists(path) else 0
    if expected is not None and have == expected:
        print(f"{label} already complete ({fmt_size(expected)}), skipped")
        return "skipped"
    if expected is not None and have > expected:
        print(f"{label} local file larger than expected; restarting")
        have = 0

    for attempt in range(retries + 1):
        headers = {"Range": f"bytes={have}-"} if have else {}
        try:
            resp = open_url(url, headers=headers, timeout=timeout, retries=0)
        except urllib.error.HTTPError as e:
            if e.code == 416 and have:      # nothing left to serve
                print(f"{label} already complete ({fmt_size(have)}), skipped")
                return "skipped"
            if e.code in REJECT_CODES and have:
                # Every header profile was refused, but this was a Range
                # request. Edge caches that will happily serve a whole object
                # sometimes reject a partial one, so give up the resume rather
                # than the file and start over.
                print(f"{label} HTTP {e.code} on the resume request; "
                      "restarting from the beginning")
                have = 0
                continue
            if e.code in RETRY_CODES and attempt < retries:
                print(f"{label} HTTP {e.code}; retrying")
                time.sleep(2 ** attempt)
                continue
            print(f"{label} FAILED: HTTP {e.code} {e.reason}")
            if e.code in REJECT_CODES:
                print(blocked_hint(url, e.code))
            return "failed"
        except FetchError as e:
            print(f"{label} FAILED: {e}")
            return "failed"

        try:
            done = _stream(resp, path, have, expected, label)
        except TRANSIENT as e:
            have = os.path.getsize(path) if os.path.exists(path) else 0
            if attempt < retries:
                print(f"\n{label} interrupted ({e}); resuming from "
                      f"{fmt_size(have)}")
                time.sleep(2 ** attempt)
                continue
            print(f"\n{label} FAILED: {net_error(url, e)}")
            return "failed"

        if expected is not None and done != expected:
            have = done
            if attempt < retries:
                print(f"{label} short read; resuming from {fmt_size(have)}")
                continue
            print(f"{label} INCOMPLETE ({fmt_size(done)} of "
                  f"{fmt_size(expected)}), re-run to resume")
            return "failed"
        return "done"
    return "failed"


def _stream(resp, path, have, expected, label):
    """Copy one response body onto path. Returns total bytes on disk."""
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
    return done


# ---- CLI --------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Download a dataset record (or a direct file URL) into the "
                    "layout the ingest scripts expect. See the module "
                    "docstring / README 'Bring your own dataset'.")
    ap.add_argument("source", help="record id/URL/DOI/ark, or direct file URL")
    ap.add_argument("--dest", default=None,
                    help="target directory (default: ./<record-id>, or . for "
                         "a direct file URL)")
    ap.add_argument("--filter", default=None, metavar="SUBSTRING",
                    help="only files whose path contains SUBSTRING "
                         "(case-insensitive), e.g. CBBT-Directional or 1.4MHz")
    ap.add_argument("--list", action="store_true",
                    help="summarise the record by folder and exit")
    ap.add_argument("--long", action="store_true",
                    help="list every matching file with size and URL, and exit")
    ap.add_argument("--tree", action="store_true",
                    help="print the record's folder structure and exit")
    ap.add_argument("--first", type=int, default=None, metavar="N",
                    help="only the N smallest matching captures. The quickest "
                         "way to try a record without pulling all of it. A "
                         "multi-file capture (.sigmf-meta plus .sigmf-data) "
                         "counts as one and is never split")
    ap.add_argument("--flat", action="store_true",
                    help="ignore record folder structure; write basenames "
                         "directly into --dest (for ingest/csv)")
    ap.add_argument("--nist-only", action="store_true",
                    help="refuse any host outside nist.gov (old behaviour)")
    ap.add_argument("--allow-host", action="append", default=[], metavar="HOST",
                    help="treat HOST as trusted; repeatable")
    ap.add_argument("--allow-http", action="store_true",
                    help="permit unencrypted http:// URLs")
    ap.add_argument("--user-agent", default=None, metavar="STRING",
                    help="send this User-Agent instead of the built-in browser "
                         "one. Only needed when a host or proxy wants a "
                         "specific string (also settable as ATLAS_USER_AGENT)")
    ap.add_argument("--retries", type=int, default=3,
                    help="retries per transient network failure (default 3)")
    ap.add_argument("--timeout", type=float, default=60,
                    help="socket timeout in seconds (default 60)")
    args = ap.parse_args()

    global POLICY
    POLICY = Policy(nist_only=args.nist_only, allow_http=args.allow_http,
                    extra_hosts=args.allow_host)
    if args.user_agent:
        # build_headers reads it from here, so it applies to the profile ladder
        # and to redirect targets without threading the value through.
        os.environ["ATLAS_USER_AGENT"] = args.user_agent

    kind, val = parse_source(args.source)
    only_listing = args.list or args.long or args.tree

    if kind == "file":
        if args.filter or only_listing or args.flat:
            sys.exit("--filter/--list/--long/--tree/--flat only apply to records")
        name = posixpath.basename(
            urllib.parse.unquote(urllib.parse.urlsplit(val).path))
        if not name or not safe_relpath(name):
            sys.exit(f"can't derive a safe filename from {val}")
        if "." not in name:
            print(f"  note: {name} has no file extension; if this is a landing "
                  "page rather than a data file you will get HTML.")
        path = os.path.join(args.dest or ".", name)
        status = download(val, path, head_size(val, args.timeout), f"  {name}",
                          retries=args.retries, timeout=args.timeout)
        if status != "failed":
            cmd = next_step([path], args.dest or ".")
            if cmd:
                print(f"\nnext: {cmd}")
        sys.exit(0 if status != "failed" else 1)

    rec = fetch_record(val, timeout=args.timeout, retries=args.retries)
    title = (rec.get("title") or "").strip()
    print(f"record {val}: {title}")
    files, checksums = record_files(rec)
    n_all = len(files)
    if args.filter:
        needle = args.filter.lower().replace("\\", "/")
        files = [f for f in files if needle in f["filepath"].lower()]
    n_filtered = len(files)
    n_units = None
    if args.first:
        # Smallest first, so "try this record" costs megabytes, not gigabytes.
        files, n_units = smallest_units(files, args.first)
    if not files:
        msg = "no downloadable files"
        if args.filter:
            msg += (f" matching '{args.filter}' (the record has {n_all} file(s); "
                    "run with --list to see the folders, or --long for names)")
        else:
            msg += " in this record"
        sys.exit(msg)
    total = sum(f["size"] or 0 for f in files)
    known = all(f["size"] is not None for f in files)
    print(f"{len(files)} file(s), {fmt_size(total)}{'' if known else '+'} total"
          + (f" (filter: {args.filter})" if args.filter else "")
          + (f"; the {min(args.first, n_units)} smallest of {n_units} "
             f"capture(s)/file(s) matching"
             if args.first and n_filtered > len(files) else "")
          + (f"; {len(checksums)} .sha256 sidecar(s) for verification"
             if checksums else ""))

    if args.tree:
        print_tree(files)
        return
    if args.long:
        print_long(files)
        return
    if args.list:
        print_summary(files)
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
    written = []
    try:
        for i, f in enumerate(sorted(files, key=lambda f: f["filepath"]), 1):
            rel = posixpath.basename(f["filepath"]) if args.flat else f["filepath"]
            path = os.path.join(dest, *rel.split("/"))
            label = f"  [{i}/{len(files)}] {rel}"
            status = download(f["url"], path, f["size"], label,
                              retries=args.retries, timeout=args.timeout)
            if status == "done" and f["filepath"] in checksums:
                if not verify_checksum(path, checksums[f["filepath"]]):
                    status = "failed"
            if status != "failed":
                written.append(path)
            counts[status] += 1
    except KeyboardInterrupt:
        print("\n\ninterrupted. Partial files kept; "
              "re-run the same command to resume")
        sys.exit(130)

    print(f"\ndone: {counts['done']} downloaded, {counts['skipped']} already "
          f"complete, {counts['failed']} failed"
          + ("" if not counts["failed"] else ". Re-run to retry/resume"))
    cmd = next_step(written, dest)
    if cmd:
        print(f"next: {cmd}")
    else:
        print("next: run the matching ingest script "
              "(build_db.py / psd_ingest.py / pfp_ingest.py / iq_ingest.py)")
    sys.exit(1 if counts["failed"] else 0)


if __name__ == "__main__":
    try:
        main()
    except FetchError as e:
        sys.exit(f"fetch.py: {e}")
    except KeyboardInterrupt:
        sys.exit(130)
