"""test_fetch.py: prove ingest/fetch.py works, with no network access.

Spins up a throwaway HTTP server on localhost that speaks the same shape as the
NIST PDR record API (an RMM ResultData envelope whose components carry
downloadURL / filepath / size, plus .sha256 sidecars), points fetch.py at it
with ATLAS_PDR_BASE, and drives the real CLI as a subprocess.

Covers: source parsing for every documented form, the host policy and its
flags, listing modes, filtering, downloading, checksum verification, resume
after a truncated file, and the error messages for a host that is not there.

    python examples/test_fetch.py          # exits 0 = PASS
"""

import hashlib
import http.server
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FETCH = os.path.join(ROOT, "ingest", "fetch.py")
sys.path.insert(0, os.path.join(ROOT, "ingest"))

failed = 0


def check(name, ok, detail=""):
    global failed
    if not ok:
        failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{(' - ' + detail) if detail else ''}")
    return ok


# ---- the fake repository ----------------------------------------------

# filepath -> bytes. Shaped like a real record: nested folders, a couple of
# formats, and enough files that the folder summary is the readable view.
BLOBS = {}
for _cfg in ("config_0", "config_1"):
    for _bw in ("1.4MHz", "5MHz"):
        BLOBS[f"{_bw}/{_cfg}/capture.sigmf-meta"] = json.dumps({
            "global": {"core:datatype": "cf32_le", "core:sample_rate": 2e6,
                       "core:version": "1.0.0"},
            "captures": [{"core:sample_start": 0, "core:frequency": 2.412e9}],
            "annotations": [],
        }).encode()
        BLOBS[f"{_bw}/{_cfg}/capture.sigmf-data"] = os.urandom(16384)
BLOBS["docs/readme.txt"] = b"a record-level readme\n"


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/octet-stream"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        path = self.path.split("?")[0]
        if path.startswith("/rmm/records/"):
            rid = path[len("/rmm/records/"):]
            if rid not in ("mds2-test", "mds2-alt"):
                return self._send(404, b'{"ResultCount":0,"ResultData":[]}',
                                  "application/json")
            # mds2-alt serves the same bytes under a different hostname, so the
            # test can allow the manifest host and still refuse the file host.
            host = self.headers["Host"]
            if rid == "mds2-alt":
                host = "localhost:" + host.split(":")[-1]
            base = f"http://{host}/files/"
            comps = []
            for fp, data in sorted(BLOBS.items()):
                comps.append({"filepath": fp, "downloadURL": base + fp,
                              "size": len(data), "@type": ["nrdp:DataFile"]})
                comps.append({"filepath": fp + ".sha256",
                              "downloadURL": base + fp + ".sha256",
                              "size": 64, "@type": ["nrdp:ChecksumFile"]})
            comps.append({"@type": ["nrdp:Subcollection"], "filepath": "1.4MHz"})
            body = json.dumps({"ResultCount": 1, "ResultData": [
                {"title": "Synthetic test record", "components": comps}]}).encode()
            return self._send(200, body, "application/json")
        if path.startswith("/files/"):
            fp = path[len("/files/"):]
            if fp.endswith(".sha256"):
                real = fp[:-len(".sha256")]
                if real not in BLOBS:
                    return self._send(404, b"no")
                digest = hashlib.sha256(BLOBS[real]).hexdigest()
                return self._send(200, f"{digest}  {os.path.basename(real)}\n".encode())
            if fp not in BLOBS:
                return self._send(404, b"no")
            data = BLOBS[fp]
            rng = self.headers.get("Range")           # resume support
            if rng and rng.startswith("bytes="):
                start = int(rng[len("bytes="):].split("-")[0])
                if start >= len(data):
                    self.send_response(416)
                    self.end_headers()
                    return
                self.send_response(206)
                self.send_header("Content-Range",
                                 f"bytes {start}-{len(data)-1}/{len(data)}")
                self.send_header("Content-Length", str(len(data) - start))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data[start:])
                return
            return self._send(200, data)
        self._send(404, b"no")


def run(args, env_extra=None, expect=None):
    env = {**os.environ, **(env_extra or {})}
    p = subprocess.run([sys.executable, FETCH] + args, env=env,
                       capture_output=True, text=True, timeout=120)
    out = p.stdout + p.stderr
    if expect is not None and p.returncode != expect:
        print(f"      (exit {p.returncode}, expected {expect})\n{out}")
    return p.returncode, out


def main():
    print("fetch.py offline test\n")

    # ---- 1. source parsing, in-process (no server needed) ----
    import fetch                                            # noqa: E402
    R = ("record", "mds2-3177")
    cases = {
        # ids
        "mds2-3177": R,
        "ark:/88434/mds2-3177": R,
        "ark:88434/mds2-3177": R,
        "doi:10.18434/mds2-3177": R,
        "10.18434/mds2-3177": R,
        # what the PDR landing page puts in the address bar
        "https://data.nist.gov/od/id/mds2-3177": R,
        "https://data.nist.gov/od/id/mds2-3177/": R,
        "https://data.nist.gov/od/id/ark:/88434/mds2-3177": R,
        "https://data.nist.gov/od/id/ark:/88434/mds2-3177/": R,
        "https://data.nist.gov/od/id/mds2-3177#files": R,
        "https://data.nist.gov/od/id/mds2-3177?selectedItemId=x": R,
        "https://data.nist.gov/rmm/records/mds2-3177": R,
        "https://data.nist.gov/rmm/records?@id=ark:/88434/mds2-3177": R,
        "https://doi.org/10.18434/mds2-3177": R,
        "http://doi.org/10.18434/mds2-3177": R,
        "http://data.nist.gov/od/id/mds2-3177": R,
        # the record-level download endpoint is a record, not a file
        "https://data.nist.gov/od/ds/mds2-3177": R,
        "https://data.nist.gov/od/ds/ark:/88434/mds2-3177": R,
        # scheme dropped by the paste
        "data.nist.gov/od/id/mds2-3177": R,
        "data.nist.gov/od/id/mds2-3177?x=1#f": R,
        # punctuation that rides along from a browser, chat or markdown
        "  https://data.nist.gov/od/id/mds2-3177  ": R,
        "<https://data.nist.gov/od/id/mds2-3177>": R,
        '"https://data.nist.gov/od/id/mds2-3177"': R,
        "'mds2-3177'": R,
        "https://data.nist.gov/od/id/mds2-3177,": R,
        "[mds2-3177](https://data.nist.gov/od/id/mds2-3177)": R,
        # direct file URLs, NIST and not
        "https://data.nist.gov/od/ds/ark:/88434/mds2-3177/1.4MHz/c0/x.tdms":
            ("file", "https://data.nist.gov/od/ds/ark:/88434/mds2-3177/1.4MHz/c0/x.tdms"),
        "https://nist-oar-cache.s3.amazonaws.com/x.tdms":
            ("file", "https://nist-oar-cache.s3.amazonaws.com/x.tdms"),
        "https://zenodo.org/records/1/files/x.npy":
            ("file", "https://zenodo.org/records/1/files/x.npy"),
    }
    bad = []
    for src, want in cases.items():
        try:
            got = fetch.parse_source(src)
        except Exception as e:                              # noqa: BLE001
            got = f"raised {type(e).__name__}"
        if got != want:
            bad.append(f"{src} -> {got}, wanted {want}")
    check(f"parse_source handles all {len(cases)} real-world paste forms",
          not bad, "; ".join(bad))

    # host policy
    default, strict = fetch.Policy(), fetch.Policy(nist_only=True)
    allowed = fetch.Policy(nist_only=True, extra_hosts=["zenodo.org"])
    check("default policy allows a non-NIST https host",
          default.allows("https://zenodo.org/x.npy"))
    check("default policy still refuses plaintext http",
          not default.allows("http://zenodo.org/x.npy"))
    check("--allow-http permits http",
          fetch.Policy(allow_http=True).allows("http://127.0.0.1:1/x"))
    check("--nist-only refuses a non-NIST host",
          not strict.allows("https://zenodo.org/x.npy"))
    check("--nist-only still allows the OAR cache bucket",
          strict.allows("https://nist-oar-cache.s3.amazonaws.com/x.tdms"))
    check("--allow-host re-permits a host under --nist-only",
          allowed.allows("https://zenodo.org/x.npy"))
    why = default.refusal("http://data.nist.gov/x")
    check("http:// refusal names the scheme, not the host",
          why is not None and "unencrypted" in why and "not a NIST host" not in why,
          (why or "").splitlines()[-1].strip())

    # ---- 2. drive the real CLI against a local fake PDR ----
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    env = {"ATLAS_PDR_BASE": f"http://127.0.0.1:{port}/rmm/records/{{}}"}
    tmp = tempfile.mkdtemp(prefix="atlas-fetch-test-")
    try:
        rc, out = run(["mds2-test", "--list", "--allow-http"], env, expect=0)
        check("--list summarises by folder", rc == 0 and "folder(s):" in out
              and "docs/" in out and "9 file(s)" in out,
              out.strip().splitlines()[-1] if out else "")

        rc, out = run(["mds2-test", "--tree", "--allow-http"], env, expect=0)
        check("--tree prints the folder structure",
              rc == 0 and "1.4MHz/" in out and "config_0/" in out)

        rc, out = run(["mds2-test", "--long", "--allow-http",
                       "--filter", "1.4MHz/config_0"], env, expect=0)
        check("--long + --filter narrows to one folder",
              rc == 0 and out.count("/files/") == 2
              and "5MHz" not in out.replace("1.4MHz", ""), f"{out.count('/files/')} urls")

        rc, out = run(["mds2-test", "--filter", "nothing-matches",
                       "--allow-http"], env, expect=1)
        check("an empty filter result explains itself",
              rc == 1 and "run with --list" in out)

        dest = os.path.join(tmp, "iqdata")
        rc, out = run(["mds2-test", "--filter", "1.4MHz/config_0",
                       "--dest", dest, "--allow-http"], env, expect=0)
        got = sorted(os.listdir(os.path.join(dest, "1.4MHz", "config_0"))) \
            if os.path.isdir(os.path.join(dest, "1.4MHz", "config_0")) else []
        check("downloads preserve the record's folder structure",
              rc == 0 and got == ["capture.sigmf-data", "capture.sigmf-meta"],
              str(got))
        check("checksums are verified and the run reports 2 downloaded",
              "2 downloaded" in out and "failed" in out and "1 failed" not in out)
        check("it names the next command to run",
              "next: python ingest/iq_ingest.py" in out,
              next((l for l in out.splitlines() if l.startswith("next:")), ""))

        rc, out = run(["mds2-test", "--filter", "1.4MHz/config_0",
                       "--dest", dest, "--allow-http"], env, expect=0)
        check("a second run skips completed files", "2 already complete" in out)

        part = os.path.join(dest, "1.4MHz", "config_0", "capture.sigmf-data")
        with open(part, "r+b") as f:
            f.truncate(1000)
        rc, out = run(["mds2-test", "--filter", "1.4MHz/config_0",
                       "--dest", dest, "--allow-http"], env, expect=0)
        want = BLOBS["1.4MHz/config_0/capture.sigmf-data"]
        check("a truncated file resumes over HTTP Range",
              rc == 0 and os.path.getsize(part) == len(want)
              and want == open(part, "rb").read(),
              f"{os.path.getsize(part)} of {len(want)} bytes")

        flat = os.path.join(tmp, "flat")
        rc, out = run(["mds2-test", "--filter", "readme", "--dest", flat,
                       "--flat", "--allow-http"], env, expect=0)
        check("--flat writes basenames straight into --dest",
              rc == 0 and os.path.exists(os.path.join(flat, "readme.txt")))

        rc, out = run(["mds2-test", "--list"], env, expect=1)
        check("without --allow-http the http fixture is refused clearly",
              rc == 1 and "unencrypted" in out and "--allow-http" in out)

        rc, out = run(["mds2-test", "--list", "--allow-http", "--nist-only"],
                      env, expect=1)
        check("--nist-only refuses the record host and names the flag",
              rc == 1 and "not a NIST host" in out
              and "--allow-host 127.0.0.1" in out)

        rc, out = run(["mds2-alt", "--list", "--allow-http", "--nist-only",
                       "--allow-host", "127.0.0.1"], env, expect=1)
        check("--nist-only drops off-host components and says how to override",
              rc == 1 and "dropped by the host policy" in out
              and "--allow-host localhost" in out,
              next((l for l in out.splitlines() if "dropped" in l), ""))

        alt = os.path.join(tmp, "alt")
        rc, out = run(["mds2-alt", "--filter", "readme", "--dest", alt,
                       "--allow-http"], env, expect=0)
        check("by default those same components download fine",
              rc == 0 and os.path.exists(
                  os.path.join(alt, "docs", "readme.txt"))
              and "note: localhost is not a NIST host" in out)

        rc, out = run(["mds2-nosuch", "--list", "--allow-http"], env, expect=1)
        check("a missing record gives one sentence, not a traceback",
              rc == 1 and "Traceback" not in out and "no record" in out)

        rc, out = run(["--retries", "0",
                       "https://not-a-real-host.invalid/x.tdms"], None, expect=1)
        check("an unreachable host gives one sentence, not a traceback",
              rc == 1 and "Traceback" not in out
              and ("cannot resolve" in out or "cannot reach" in out),
              out.strip().splitlines()[-1] if out else "")
    finally:
        srv.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if failed:
        print(f"RESULT: FAIL ({failed} check(s) failed above)")
        return 1
    print("RESULT: PASS - fetch.py verified offline against a local fixture")
    return 0


if __name__ == "__main__":
    sys.exit(main())
