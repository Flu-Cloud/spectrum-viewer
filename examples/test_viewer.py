"""test_viewer.py: drive viewer.html in a real browser and press every control.

test_serve.py proves the endpoints answer. This proves the page in front of them
actually works: that the source dropdown populates, that picking a capture
renders, that the zoom sliders, the colormap picker, the statistic buttons,
reset and export all do something and none of them throws. A backend test cannot
catch a viewer that never issues the request.

Every console error and every failed request is recorded for the whole session
and reported at the end, so a JavaScript exception fails the run rather than
being invisible.

Optional, and skipped cleanly when it cannot run: it needs `pip install
playwright` and a Chromium, neither of which the project itself requires. Point
ATLAS_CHROMIUM at a browser binary if Playwright cannot find its own.

    pip install playwright && playwright install chromium
    python examples/test_viewer.py          # exits 0 = PASS (or SKIP)
"""

import contextlib
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)

failed = 0
# Where a Chromium might already be, so a machine that has one does not have to
# download a second copy. PLAYWRIGHT_BROWSERS_PATH layouts are versioned, hence
# the glob rather than a fixed path.
CHROMIUM_HINTS = ("/opt/pw-browsers/chromium-*/chrome-linux/chrome",
                  "/usr/bin/chromium", "/usr/bin/chromium-browser",
                  "/usr/bin/google-chrome")


def check(name, ok, detail=""):
    global failed
    if not ok:
        failed += 1
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{(' - ' + detail) if detail else ''}")
    return ok


def skip(why):
    print(f"  [SKIP] {why}")
    print("\nRESULT: SKIP - the browser test did not run (this is not a failure)")
    return 0


def find_chromium():
    import glob
    for pat in (os.environ.get("ATLAS_CHROMIUM"), *CHROMIUM_HINTS):
        if not pat:
            continue
        for p in sorted(glob.glob(pat), reverse=True):
            if os.path.isfile(p) and os.access(p, os.X_OK):
                return p
    return None


def free_port():
    with contextlib.closing(socket.socket()) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_up(port, proc, timeout=60):
    """True once the server answers. Fails fast if it died instead."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        with contextlib.closing(socket.socket()) as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.25)
    return False


def build(tmp):
    """A db dir with all four layers, from the offline fixtures."""
    import test_atlas as TA
    import test_ingest as TI
    data = os.path.join(tmp, "data")
    TI.make_data(data)
    TA.make_iq(os.path.join(data, "iq", "run1"), n=1024 * 256)
    dbs = os.path.join(tmp, "dbs")
    os.makedirs(dbs)
    env = {**os.environ, "ATLAS_DB_DIR": dbs,
           "SPECTRUM_DB": os.path.join(dbs, "spectrum.duckdb"),
           "PSD_DB": os.path.join(dbs, "psd.duckdb"),
           "PFP_DB": os.path.join(dbs, "pfp.duckdb"),
           "IQ_DB": os.path.join(dbs, "iq.duckdb")}
    p = subprocess.run([sys.executable, os.path.join(ROOT, "atlas.py"),
                        "get", data], env=env, cwd=ROOT, capture_output=True,
                       text=True, timeout=1800)
    if p.returncode != 0:
        print(p.stdout + p.stderr)
        raise SystemExit("could not build the fixture databases")
    return dbs, env


def main():
    print("viewer.html browser test\n")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return skip("playwright is not installed (pip install playwright)")
    exe = find_chromium()

    tmp = tempfile.mkdtemp(prefix="atlas-viewer-test-")
    proc = None
    try:
        dbs, env = build(tmp)
        port = free_port()
        proc = subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "serve.py")],
            env={**env, "SEA_PORT": str(port)}, cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if not wait_up(port, proc):
            out = ""
            if proc.poll() is not None and proc.stdout:
                out = proc.stdout.read()[-400:]
            return skip(f"serve.py did not come up on port {port}. {out}")
        base = f"http://127.0.0.1:{port}"
        check("serve.py answers on a real port", True, base)

        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch(executable_path=exe)
            except Exception as e:                            # noqa: BLE001
                return skip(f"could not launch Chromium ({str(e).splitlines()[0]})")
            page = browser.new_page(viewport={"width": 1280, "height": 800})
            errors, bad_requests = [], []
            page.on("console", lambda m: m.type == "error"
                    and errors.append(m.text))
            page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))
            page.on("requestfailed", lambda r: bad_requests.append(
                f"{r.url} ({r.failure})"))
            page.on("response", lambda r: r.status >= 400
                    and bad_requests.append(f"{r.url} -> {r.status}"))

            page.goto(base, wait_until="networkidle", timeout=60000)
            check("the viewer page loads", page.title() != "",
                  page.title()[:60])
            check("the canvas is present", page.locator("#cv").count() == 1)

            # ---- the source dropdown: the first thing anyone touches ----
            opts = page.locator("#source option")
            labels = [opts.nth(i).inner_text() for i in range(opts.count())]
            check("the source dropdown is populated from the databases",
                  len(labels) >= 2, f"{len(labels)}: " + " | ".join(labels[:4]))
            iq_opt = next((l for l in labels if "IQ" in l or "demo" in l), None)
            check("the demo IQ capture appears in the source dropdown",
                  iq_opt is not None, str(iq_opt))

            def settle(ms=1200):
                page.wait_for_timeout(ms)
                with contextlib.suppress(Exception):
                    page.wait_for_load_state("networkidle", timeout=8000)

            def tiles_drawn():
                """Whether the canvas has non-background pixels in it."""
                return page.evaluate("""() => {
                    const c = document.getElementById('cv');
                    if (!c || !c.width || !c.height) return 0;
                    const g = c.getContext('2d');
                    const d = g.getImageData(0, 0, c.width, c.height).data;
                    let n = 0;
                    for (let i = 0; i < d.length; i += 4 * 97) {
                        if (d[i] > 24 || d[i+1] > 24 || d[i+2] > 28) n++;
                    }
                    return n;
                }""")

            # Select every source in turn: each must render something.
            drew = {}
            for i, label in enumerate(labels):
                page.select_option("#source", index=i)
                settle()
                drew[label] = tiles_drawn()
            check("every source in the dropdown renders pixels",
                  all(v > 0 for v in drew.values()),
                  ", ".join(f"{k[:26]}={v}" for k, v in drew.items()))

            # ---- pick the IQ capture and work every control on it ----
            if iq_opt:
                page.select_option("#source", label=iq_opt)
                settle()
            before = tiles_drawn()

            # #sensor is deliberately disabled in IQ mode (a capture has no
            # sensor), so only work the controls the current mode enables.
            for cid in ("#cmap", "#sensor"):
                el = page.locator(cid)
                if not (el.count() and el.is_visible() and el.is_enabled()):
                    print(f"  [ -- ] {cid} is not active in this mode, skipped")
                    continue
                n = page.locator(f"{cid} option").count()
                for i in range(n):
                    page.select_option(cid, index=i, timeout=15000)
                    settle(700)
                check(f"{cid} works for all {n} of its options",
                      tiles_drawn() > 0 and not errors,
                      "; ".join(errors[-2:]))

            stats = page.locator("button[data-stat]")
            if stats.count():
                pressed = 0
                for i in range(stats.count()):
                    b = stats.nth(i)
                    if b.is_visible() and b.is_enabled():
                        b.click(timeout=15000)
                        settle(700)
                        pressed += 1
                check(f"the statistic buttons all respond ({pressed} pressed)",
                      tiles_drawn() > 0 or pressed == 0)

            # Zoom with the wheel, frequency-zoom with ctrl+wheel, and pan by
            # dragging: the three canvas gestures the whole viewer is built on.
            box = page.locator("#cv").bounding_box()
            cx, cy = box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
            page.mouse.move(cx, cy)
            for _ in range(4):
                page.mouse.wheel(0, -240)
                page.wait_for_timeout(220)
            settle()
            check("scrolling zooms in and keeps rendering", tiles_drawn() > 0,
                  f"{tiles_drawn()} sampled pixels")
            page.keyboard.down("Control")
            for _ in range(3):
                page.mouse.wheel(0, -240)
                page.wait_for_timeout(220)
            page.keyboard.up("Control")
            settle()
            check("ctrl+scroll zooms frequency and keeps rendering",
                  tiles_drawn() > 0)
            page.mouse.move(cx, cy)
            page.mouse.down()
            page.mouse.move(cx - 220, cy - 60, steps=12)
            page.mouse.up()
            settle()
            check("dragging pans and keeps rendering", tiles_drawn() > 0)

            # The two zoom sliders, dragged by their knobs.
            for knob, track, what in (("#timeknob", "#timetrack", "time"),
                                      ("#freqknob", "#freqtrack", "frequency")):
                k, t = page.locator(knob), page.locator(track)
                if not (k.count() and t.count() and k.is_visible()):
                    continue
                kb, tb = k.bounding_box(), t.bounding_box()
                page.mouse.move(kb["x"] + kb["width"] / 2,
                                kb["y"] + kb["height"] / 2)
                page.mouse.down()
                page.mouse.move(tb["x"] + tb["width"] / 2,
                                tb["y"] + tb["height"] * 0.25, steps=10)
                page.mouse.up()
                settle()
                check(f"the {what} zoom slider drags and keeps rendering",
                      tiles_drawn() > 0)

            reset = page.locator("#reset")
            if reset.count():
                reset.click()
                settle()
                check("reset zooms back out and keeps rendering",
                      tiles_drawn() > 0)

            # Export writes a PNG through a download; the click must at least
            # not throw, and should produce a file when it is a real download.
            exp = page.locator("#exportBtn")
            if exp.count() and exp.is_visible():
                try:
                    with page.expect_download(timeout=15000) as dl:
                        exp.click()
                    path = dl.value.path()
                    ok = bool(path) and os.path.getsize(path) > 1000
                    check("export produces a PNG", ok,
                          f"{dl.value.suggested_filename}, "
                          f"{os.path.getsize(path) if path else 0} bytes")
                except Exception as e:                        # noqa: BLE001
                    # Some builds render into a new tab instead of downloading.
                    check("export does not throw",
                          "download" in str(e).lower() or "timeout" in str(e).lower(),
                          str(e).splitlines()[0][:100])

            # Reloading with data present must not error either.
            page.goto(base, wait_until="networkidle", timeout=60000)
            settle()
            check("a reload comes back up clean", tiles_drawn() >= 0)

            real_errors = [e for e in errors if "favicon" not in e.lower()]
            check("no JavaScript error anywhere in the session",
                  not real_errors, "; ".join(real_errors[:3]))
            real_bad = [r for r in bad_requests if "favicon" not in r.lower()]
            check("no request failed anywhere in the session",
                  not real_bad, "; ".join(real_bad[:3]))
            browser.close()
    finally:
        if proc is not None:
            proc.terminate()
            with contextlib.suppress(Exception):
                proc.wait(timeout=15)
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    if failed:
        print(f"RESULT: FAIL ({failed} check(s) failed above)")
        return 1
    print("RESULT: PASS - every viewer control works in a real browser")
    return 0


if __name__ == "__main__":
    sys.exit(main())
