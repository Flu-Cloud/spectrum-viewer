"""serve.py: Flask backend for the spectrum viewer.

linkedin.com/in/jimmy-lu-/

One canvas, three continuous CBRS layers (summary -> PSD -> PFP) plus the
independent IQ-capture mode. For any requested window the server picks the
coarsest stored level that still fills the plot, renders it to a WebP tile
and streams the bytes directly (metadata rides in the X-Meta header).

Storage (built by build_db.py / *_ingest.py, then compact_db.py):
    spectrum.duckdb  summaries; dBm stored as SMALLINT dBm*10
    psd.duckdb       psd_chunk: zlib blobs of 256 consecutive int8 spectra
    pfp.duckdb       pfp_chunk: zlib blobs of 1024 consecutive int8 frames
    iq.duckdb        iq_stft: int8 STFT pyramid per capture (iq_ingest.py)

    cd /path/to/spectrum-viewer
    py serve.py            # http://127.0.0.1:8090

"""

import gzip
import io
import json
import math
import os
import threading
import zlib

import duckdb
import numpy as np
from PIL import Image
from flask import Flask, Response, jsonify, make_response, request, send_file

HERE = os.path.dirname(os.path.abspath(__file__))
# Each database path is overridable, like IQ_DB below, so a test run or a
# second dataset can point somewhere else without moving files around.
# ATLAS_DB_DIR moves all four databases at once; the per-database variables
# override it one at a time. atlas.py and every ingest script agree on this.
DB_DIR = os.environ.get("ATLAS_DB_DIR") or HERE
DB_PATH = os.environ.get("SPECTRUM_DB") or os.path.join(DB_DIR, "spectrum.duckdb")

# (table, bucket_seconds), finest first. 0 == raw native cadence.
LEVELS = [
    ("raw", 0),
    ("lvl_m10", 600),
    ("lvl_h1", 3600),
    ("lvl_h6", 21600),
    ("lvl_d1", 86400),
]

app = Flask(__name__)
_con_lock = threading.Lock()

# The CBRS monitoring databases (spectrum/psd/pfp .duckdb) are OPTIONAL: they are
# multi-GB and not shipped with the repo. When spectrum.duckdb is absent -- e.g. a
# fresh clone running only the IQ demo (examples/make_sample.py) -- the server
# still starts and serves IQ captures; the CBRS endpoints simply report no data.
if os.path.exists(DB_PATH):
    con = duckdb.connect(DB_PATH, read_only=True)
    # Static per-boot metadata (sensor list + channel freqs): computed once so
    # /api/meta never rescans the 47M-row raw table per page load.
    _META = {
        "sensors": [{"sensor": r[0], "t_min": r[1], "t_max": r[2], "n": r[3]}
                    for r in con.execute(
                        "SELECT sensor, t_min, t_max, n FROM meta ORDER BY sensor").fetchall()],
        "freqs": [r[0] for r in con.execute(
            "SELECT DISTINCT freq FROM raw ORDER BY freq").fetchall()],
    }
else:
    con = None
    _META = {"sensors": [], "freqs": []}
    print(f"[serve] {os.path.basename(DB_PATH)} not found -- CBRS monitoring "
          "disabled; IQ capture mode still works. Run examples/make_sample.py "
          "for a zero-download demo.")


# ---- Colormaps + tile encoding (mirrored in viewer.html) ---------------
CMAP_ANCHORS = np.array([
    [0, 0, 4], [20, 11, 52], [57, 9, 98], [95, 19, 110], [133, 33, 107],
    [169, 46, 94], [203, 65, 73], [230, 93, 47], [247, 131, 17],
    [252, 173, 18], [250, 205, 42], [252, 255, 164],
], dtype=np.float64)


class BadRequest(Exception):
    """A request that cannot be honoured as asked. Answered with 400."""


@app.errorhandler(BadRequest)
def _bad_request(e):
    return jsonify({"error": str(e)}), 400


def _num(name, default=None, cast=float, required=False):
    """One numeric query parameter, parsed.

    Every axis value arrives as text in a URL, so a missing or non-numeric one
    is the caller's mistake and deserves a 400 naming the parameter. Calling
    float() on it directly meant an unhandled exception and a 500 -- the one
    answer the viewer can do nothing with, and indistinguishable from the
    server being broken.
    """
    raw = request.args.get(name)
    if raw is None or raw == "":
        if required:
            raise BadRequest(f"missing required parameter '{name}'")
        return default
    try:
        return cast(raw)
    except (TypeError, ValueError):
        raise BadRequest(f"parameter '{name}' must be a number, got {raw!r}")


def _window(default_h=600):
    """The (t0, t1, H) every layer endpoint starts from."""
    return (_num("t0", required=True), _num("t1", required=True),
            max(1, _num("h", 600, int)))


def _locked_scale():
    """The colour lock, when the viewer sent both halves of it, else None."""
    lo, hi = _num("vmin"), _num("vmax")
    return None if lo is None or hi is None else (lo, hi)


def _build_lut(anchors):
    seg = len(anchors) - 1
    xs = np.linspace(0, seg, 256)
    k = np.clip(np.floor(xs).astype(int), 0, seg - 1)
    f = (xs - k)[:, None]
    return np.clip(anchors[k] + (anchors[k + 1] - anchors[k]) * f, 0, 255).astype(np.uint8)


def _turbo_lut():
    x = np.linspace(0, 1, 256)
    r = 34.61 + x*(1172.33 + x*(-10793.56 + x*(33300.12 + x*(-38394.49 + x*14825.05))))
    g = 23.31 + x*(557.33 + x*(1225.33 + x*(-3574.96 + x*(3520.99 + x*-1300.91))))
    b = 27.2 + x*(3211.1 + x*(-15327.97 + x*(27814.0 + x*(-22569.18 + x*6838.66))))
    return np.clip(np.stack([r, g, b], axis=1), 0, 255).astype(np.uint8)


LUTS = {"inferno": _build_lut(CMAP_ANCHORS), "turbo": _turbo_lut()}


def _colorize(mat, vmin, vmax, cmap="inferno"):
    lut = LUTS.get(cmap, LUTS["inferno"])
    idx = np.clip((mat - vmin) / max(vmax - vmin, 1e-6) * 255, 0, 255)
    nan = ~np.isfinite(mat)
    idx[nan] = 0
    rgb = lut[idx.astype(np.uint8)]
    rgb[nan] = (8, 9, 12)
    return rgb


def _tile(rgb, meta):
    """WebP tile bytes + metadata in the X-Meta header (no base64/JSON body:
    ~25% fewer bytes and the browser decodes the image natively)."""
    buf = io.BytesIO()
    # q80/method=4: visually identical to lossless for spectrograms and ~7x
    # faster to encode than method=6; encode speed dominates felt zoom latency.
    Image.fromarray(rgb, "RGB").save(buf, format="WEBP", quality=80, method=4)
    resp = Response(buf.getvalue(), mimetype="image/webp")
    resp.headers["X-Meta"] = json.dumps(meta)
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


def _gz(resp):
    """gzip a JSON response when the client accepts it (summary payloads are
    a few hundred KB of number arrays -> ~4x smaller)."""
    if "gzip" in request.headers.get("Accept-Encoding", "") and resp.status_code == 200:
        resp.set_data(gzip.compress(resp.get_data(), 5))
        resp.headers["Content-Encoding"] = "gzip"
    return resp


def pick_level(span_seconds, target_cols):
    """Finest level whose bucket count over this span <= target_cols."""
    for tbl, bucket in LEVELS:
        if bucket == 0:
            if span_seconds <= 6 * 3600:   # raw only when the window is cheap
                return tbl, bucket
            continue
        if span_seconds / bucket <= target_cols:
            return tbl, bucket
    return LEVELS[-1]


@app.route("/")
def index():
    resp = make_response(send_file(os.path.join(HERE, "viewer.html")))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    return resp


@app.route("/api/meta")
def meta():
    return jsonify(_META)


@app.route("/api/heatmap")
def heatmap():
    if con is None:                     # no CBRS database present (IQ-only install)
        return _gz(jsonify({"level": None, "bucket": 0, "count": 0,
                            "freq": [], "t": [], "max": [], "median": [], "mean": []}))
    sensor = request.args.get("sensor")
    t0 = _num("t0", required=True)
    t1 = _num("t1", required=True)
    width = max(1, _num("width", 1600, int))
    span = max(t1 - t0, 1.0)

    tbl, bucket = pick_level(span, width)
    with _con_lock:
        rows = con.execute(f"""
            SELECT freq, t, mx, md, mn
            FROM {tbl}
            WHERE sensor = ? AND t >= ? AND t < ?
            ORDER BY t
        """, [sensor, t0, t1]).fetchall()

    # Columnar payload; dBm stored as SMALLINT dBm*10 -> /10 restores 0.1 dBm.
    freq, t, mx, md, mn = [], [], [], [], []
    for r in rows:
        freq.append(r[0]); t.append(int(r[1]))
        mx.append(None if r[2] is None else r[2] / 10.0)
        md.append(None if r[3] is None else r[3] / 10.0)
        mn.append(None if r[4] is None else r[4] / 10.0)
    return _gz(jsonify({
        "level": tbl, "bucket": bucket, "count": len(rows),
        "freq": freq, "t": t, "max": mx, "median": md, "mean": mn,
    }))


# ---- PSD continuous layer (chunked int8 spectra, zlib) -----------------
PSD_DB = os.environ.get("PSD_DB") or os.path.join(DB_DIR, "psd.duckdb")
F0 = 3530040000.0     # first PSD bin (Hz)
DF = 80000.0          # bin spacing (Hz)
NF = 2250             # bins  (full band 3530.04 .. 3709.96 MHz)
# PSD is dBm/Hz; summaries are channel power (dBm). +10*log10(10 MHz) puts the
# PSD layer on the SAME dBm scale so colours line up across the zoom boundary.
PSD_DBM_OFFSET = 70.0

_psd_con = None
_psd_lock = threading.Lock()
_psd_scale_cache = {}   # sensor -> (vmin, vmax) sampled across the whole range
_psd_kind = None        # 'chunk' | 'rows' | None
_psd_init = threading.Lock()


def table_names(c):
    try:
        return {r[0] for r in c.execute(
            "SELECT table_name FROM information_schema.tables").fetchall()}
    except Exception:
        return set()


def psd_conn():
    """Persistent read-only handle (saves the ~15ms reopen per zoom). The live
    DB is never written while serving; ingest builds a fresh file and swaps."""
    global _psd_con, _psd_kind
    if _psd_con is not None or not os.path.exists(PSD_DB):
        return _psd_con
    with _psd_init:                 # two requests can race here on first load
        if _psd_con is not None:
            return _psd_con
        try:
            _psd_con = duckdb.connect(PSD_DB, read_only=True)
        except Exception:
            return None
        # Two on-disk shapes are valid: the compact chunk schema written by
        # compact_db.py, and the row-per-capture schema psd_ingest.py writes.
        # Reading both means compaction is a size optimisation, not a step you
        # can forget and end up with a viewer that renders nothing.
        t = table_names(_psd_con)
        _psd_kind = "chunk" if "psd_chunk" in t else ("rows" if "psd" in t else None)
        if _psd_kind == "rows":
            print("[serve] psd.duckdb is the uncompacted row schema; serving it "
                  "directly. Run ingest/compact_db.py to shrink it.")
    return _psd_con


def _unpack(rows, width):
    """(times, mat) from chunk rows of (n, times_blob, data_blob)."""
    ts = np.concatenate([np.frombuffer(zlib.decompress(r[1]), np.float64) for r in rows])
    mat = np.concatenate([np.frombuffer(zlib.decompress(r[2]), np.uint8).reshape(r[0], width)
                          for r in rows])
    return ts, mat


def _unpack_rows(rows, width):
    """(times, mat) from row-per-capture rows of (t, blob)."""
    ts = np.array([r[0] for r in rows], dtype=np.float64)
    mat = np.stack([np.frombuffer(r[1], np.uint8) for r in rows]) if rows \
        else np.zeros((0, width), np.uint8)
    return ts, mat.reshape(len(rows), width)


def _psd_scale(c, sensor, qmin, qmax):
    sc = _psd_scale_cache.get(sensor)
    if sc is not None:
        return sc
    if _psd_kind == "chunk":
        rows = c.execute("SELECT n, times, specs FROM psd_chunk WHERE sensor=? "
                         "USING SAMPLE reservoir(4 ROWS)", [sensor]).fetchall()
        specs = _unpack(rows, NF)[1] if rows else None
    else:
        rows = c.execute("SELECT t, spec FROM psd WHERE sensor=? "
                         "USING SAMPLE reservoir(400 ROWS)", [sensor]).fetchall()
        specs = _unpack_rows(rows, NF)[1] if rows else None
    if specs is None or not len(specs):
        sc = (qmin, qmax)
    else:
        specs = specs[:: max(1, len(specs) // 400)]
        full = qmin + (specs.astype(np.float32) / 255.0) * (qmax - qmin) + PSD_DBM_OFFSET
        sc = (float(np.percentile(full, 2)), float(np.percentile(full, 98)))
    _psd_scale_cache[sensor] = sc
    return sc


def _psd_window(c, sensor, t0, t1):
    """Spectra in [t0,t1). Data gap -> hold the nearest capture (matches the
    summary layer's gap-fill so zooming keeps showing last-known data)."""
    if _psd_kind == "chunk":
        rows = c.execute("SELECT n, times, specs FROM psd_chunk WHERE sensor=? "
                         "AND t1>=? AND t0<? ORDER BY t0", [sensor, t0, t1]).fetchall()
        if rows:
            ts, specs = _unpack(rows, NF)
            m = (ts >= t0) & (ts < t1)
            if m.any():
                return specs[m], False
        row = c.execute("SELECT n, times, specs FROM psd_chunk WHERE sensor=? AND t0<? "
                        "ORDER BY t0 DESC LIMIT 1", [sensor, t1]).fetchone()
        if row is None:
            row = c.execute("SELECT n, times, specs FROM psd_chunk WHERE sensor=? "
                            "ORDER BY t0 LIMIT 1", [sensor]).fetchone()
        if row is None:
            return None, False
        ts, specs = _unpack([row], NF)
        i = max(0, int(np.searchsorted(ts, t1)) - 1)
        return specs[i:i + 1], True

    rows = c.execute("SELECT t, spec FROM psd WHERE sensor=? AND t>=? AND t<? "
                     "ORDER BY t", [sensor, t0, t1]).fetchall()
    if rows:
        return _unpack_rows(rows, NF)[1], False
    row = c.execute("SELECT t, spec FROM psd WHERE sensor=? AND t<? "
                    "ORDER BY t DESC LIMIT 1", [sensor, t1]).fetchone()
    if row is None:
        row = c.execute("SELECT t, spec FROM psd WHERE sensor=? ORDER BY t LIMIT 1",
                        [sensor]).fetchone()
    if row is None:
        return None, False
    return _unpack_rows([row], NF)[1], True


@app.route("/api/psd_meta")
def psd_meta():
    c = psd_conn()
    if c is None or _psd_kind is None:
        return jsonify({"has": False})
    with _psd_lock:
        row = c.execute("SELECT f0, df, nf, t_min, t_max FROM psd_meta WHERE sensor=?",
                        [request.args.get("sensor")]).fetchone()
    # An ingest that read no files leaves a meta row with a null time range.
    # Never advertise a layer whose backing rows are not actually there.
    if not row or row[3] is None or row[4] is None:
        return jsonify({"has": False})
    f0, df, nf, tmin, tmax = row
    return jsonify({"has": True, "fmin": f0, "fmax": f0 + (nf - 1) * df,
                    "t_min": tmin, "t_max": tmax})


@app.route("/api/psd_layer")
def psd_layer():
    sensor = request.args.get("sensor")
    t0, t1, H = _window()
    f0 = _num("f0", 0.0); f1 = _num("f1", 0.0)
    fi0 = int(round((f0 - F0) / DF)) if f0 > 0 else 0
    fi1 = int(round((f1 - F0) / DF)) if f1 > 0 else NF - 1
    fi0 = max(0, min(NF - 1, fi0)); fi1 = max(0, min(NF - 1, fi1))
    if fi1 <= fi0:
        fi0, fi1 = 0, NF - 1
    c = psd_conn()
    if c is None or _psd_kind is None:
        return jsonify({"error": "psd layer not ready"}), 503
    with _psd_lock:
        specs, gap = _psd_window(c, sensor, t0, t1)
        mrow = c.execute("SELECT qmin, qmax FROM psd_meta WHERE sensor=?", [sensor]).fetchone()
    if specs is None:
        return jsonify({"error": "no psd data for this sensor"}), 404
    qmin, qmax = mrow or (-180.0, -90.0)
    ncap = len(specs)
    band = specs[:, fi0:fi1 + 1].astype(np.float32)   # (ncap, nbins)
    nbins = band.shape[1]
    fb = max(1, math.ceil(nbins / H))                 # freq bins per pixel row
    pad = (-nbins) % fb
    if pad:
        band = np.pad(band, ((0, 0), (0, pad)))
    nF = band.shape[1] // fb
    binned = band.reshape(ncap, nF, fb).max(axis=2)
    img = qmin + (binned.T / 255.0) * (qmax - qmin) + PSD_DBM_OFFSET  # (nF, ncap); row 0 = low f
    locked = _locked_scale()
    if locked:
        vmin, vmax = locked
    else:
        with _psd_lock:
            vmin, vmax = _psd_scale(c, sensor, qmin, qmax)   # full-range: no drift on zoom
    return _tile(_colorize(img, vmin, vmax, request.args.get("cmap", "inferno")), {
        "t0": t0, "t1": t1,
        "fmin": F0 + fi0 * DF, "fmax": F0 + fi1 * DF,
        "ncap": int(ncap), "cols": int(ncap), "nf": int(nF),
        "vmin": round(vmin, 1), "vmax": round(vmax, 1), "gap": gap,
    })


# ---- PFP layer (periodic-frame-power, ~18 us within a 10 ms frame) ----
PFP_DB = os.environ.get("PFP_DB") or os.path.join(DB_DIR, "pfp.duckdb")
_pfp_con = None
_pfp_lock = threading.Lock()
_pfp_freqs_cache = {}
_pfp_kind = None        # 'chunk' | 'rows' | None
_pfp_init = threading.Lock()


def pfp_conn():
    global _pfp_con, _pfp_kind
    if _pfp_con is not None or not os.path.exists(PFP_DB):
        return _pfp_con
    with _pfp_init:                 # same first-load race as PSD above
        if _pfp_con is not None:
            return _pfp_con
        try:
            _pfp_con = duckdb.connect(PFP_DB, read_only=True)
        except Exception:
            return None
        t = table_names(_pfp_con)       # same dual-schema rule as PSD above
        _pfp_kind = "chunk" if "pfp_chunk" in t else ("rows" if "pfp" in t else None)
        if _pfp_kind == "rows":
            print("[serve] pfp.duckdb is the uncompacted row schema; serving it "
                  "directly. Run ingest/compact_db.py to shrink it.")
    return _pfp_con


@app.route("/api/pfp_meta")
def pfp_meta():
    sensor = request.args.get("sensor")
    c = pfp_conn()
    if c is None or _pfp_kind is None:
        return jsonify({"has": False})
    with _pfp_lock:
        m = c.execute("SELECT npos, frame_ms, t_min, t_max, stat FROM pfp_meta WHERE sensor=?",
                      [sensor]).fetchone()
        if not m or m[2] is None or m[3] is None:
            return jsonify({"has": False})
        freqs = _pfp_freqs_cache.get(sensor)
        if freqs is None:
            src = "pfp_chunk" if _pfp_kind == "chunk" else "pfp"
            freqs = [r[0] for r in c.execute(
                f"SELECT DISTINCT freq FROM {src} WHERE sensor=? ORDER BY 1",
                [sensor]).fetchall()]
            _pfp_freqs_cache[sensor] = freqs
    return jsonify({"has": True, "npos": m[0], "frame_ms": m[1],
                    "t_min": m[2], "t_max": m[3], "stat": m[4], "freqs": freqs})


@app.route("/api/pfp_frame")
def pfp_frame():
    """Frame heatmap for one channel: X = capture time, Y = frame position."""
    sensor = request.args.get("sensor")
    freq = _num("freq", required=True)
    t0, t1, H = _window()
    c = pfp_conn()
    if c is None or _pfp_kind is None:
        return jsonify({"error": "pfp not ready"}), 503
    with _pfp_lock:
        if _pfp_kind == "chunk":
            rows = c.execute(
                "SELECT n, times, frames FROM pfp_chunk WHERE sensor=? AND freq=? "
                "AND t1>=? AND t0<? ORDER BY t0", [sensor, freq, t0, t1]).fetchall()
        else:
            rows = c.execute(
                "SELECT t, frame FROM pfp WHERE sensor=? AND freq=? "
                "AND t>=? AND t<? ORDER BY t", [sensor, freq, t0, t1]).fetchall()
        m = c.execute("SELECT npos, frame_ms, qmin, qmax FROM pfp_meta WHERE sensor=?",
                      [sensor]).fetchone()
    if not rows or not m:
        return jsonify({"error": "no pfp in window"}), 404
    npos, frame_ms, qmin, qmax = m
    ts, frames = (_unpack(rows, npos) if _pfp_kind == "chunk"
                  else _unpack_rows(rows, npos))
    keep = (ts >= t0) & (ts < t1)
    if not keep.any():
        return jsonify({"error": "no pfp in window"}), 404
    ts = ts[keep]; frames = frames[keep]
    ncap = len(frames)
    mat = frames.T.astype(np.float32)                 # (npos, ncap)
    nrows = min(H, npos)
    if npos > nrows:
        fb = math.ceil(npos / nrows)
        pad = (-npos) % fb
        m2 = np.pad(mat, ((0, pad), (0, 0))) if pad else mat
        mat = m2.reshape(m2.shape[0] // fb, fb, ncap).max(axis=1)
    img = qmin + (mat / 255.0) * (qmax - qmin)        # (nrows, ncap); row 0 = frame start
    locked = _locked_scale()
    if locked:
        vmin, vmax = locked
    else:
        vmin = float(np.percentile(img, 2)); vmax = float(np.percentile(img, 98))
    return _tile(_colorize(img, vmin, vmax, request.args.get("cmap", "inferno")), {
        "t0": float(ts[0]), "t1": float(ts[-1]), "ncap": ncap,
        "npos": npos, "nrows": int(img.shape[0]), "frame_ms": frame_ms, "freq": freq,
        "vmin": round(vmin, 1), "vmax": round(vmax, 1),
    })


# ---- IQ capture mode (independent SigMF/TDMS captures; own axes) ------
# Per-request read-only connections on purpose: iq_ingest.py may be appending
# new captures between requests, and a held handle would block its write lock.
IQ_DB = os.environ.get("IQ_DB") or os.path.join(DB_DIR, "iq.duckdb")


def iq_conn():
    if not os.path.exists(IQ_DB):
        return None
    try:
        return duckdb.connect(IQ_DB, read_only=True)
    except Exception:
        return None


@app.route("/api/iq_index")
def iq_index():
    c = iq_conn()
    if c is None:
        return jsonify({"captures": []})
    rows = c.execute("SELECT id, name, dataset, fc, fs, duration FROM iq_meta "
                     "ORDER BY dataset, name").fetchall()
    c.close()
    return jsonify({"captures": [
        {"id": r[0], "name": r[1], "dataset": r[2], "fc": r[3], "fs": r[4],
         "duration": r[5]} for r in rows]})


@app.route("/api/iq_meta")
def iq_meta():
    c = iq_conn()
    row = c and c.execute(
        "SELECT id, dataset, name, fc, fs, duration, n_samples, nfft, nfreq, "
        "hop, nlevels, vmin, vmax FROM iq_meta WHERE id=?",
        [request.args.get("id")]).fetchone()
    if c:
        c.close()
    if not row:
        return jsonify({"has": False})
    k = ["id", "dataset", "name", "fc", "fs", "duration", "n_samples", "nfft",
         "nfreq", "hop", "nlevels", "vmin", "vmax"]
    return jsonify({"has": True, **dict(zip(k, row))})


@app.route("/api/iq_layer")
def iq_layer():
    """WebP tile of one capture's spectrogram for a [t0,t1]x[f0,f1] window.
    Picks the finest pyramid level with <= ~2*w columns (mirrors pick_level)."""
    cid = request.args.get("id")
    c = iq_conn()
    if c is None:
        return jsonify({"error": "iq layer not ready"}), 503
    m = c.execute("SELECT fc, fs, duration, nfft, hop, nlevels, qmin, qmax, "
                  "vmin, vmax FROM iq_meta WHERE id=?", [cid]).fetchone()
    if not m:
        c.close()
        return jsonify({"error": "unknown capture"}), 404
    fc, fs, dur, nfft, hop, nlevels, qmin, qmax, dvmin, dvmax = m
    t0 = max(0.0, _num("t0", 0.0))
    t1 = min(dur, _num("t1", dur))
    W = max(1, _num("w", 1200, int))
    H = max(1, _num("h", 600, int))
    fmin_full, fmax_full = fc - fs / 2, fc + fs / 2
    f0 = max(fmin_full, _num("f0", fmin_full))
    f1 = min(fmax_full, _num("f1", fmax_full))
    if t1 <= t0 or f1 <= f0:
        c.close()
        return jsonify({"error": "empty window"}), 400
    span_cols0 = (t1 - t0) * fs / hop
    level = 0
    while level < nlevels - 1 and span_cols0 / (1 << level) > 2 * W:
        level += 1
    colw = hop * (1 << level) / fs                    # seconds per column
    c0 = int(t0 / colw)
    c1 = max(c0 + 1, int(math.ceil(t1 / colw)))
    rows = c.execute(
        "SELECT col0, ncols, chunk FROM iq_stft WHERE id=? AND level=? "
        "AND col0 < ? AND col0 + ncols > ? ORDER BY col0",
        [cid, level, c1, c0]).fetchall()
    c.close()
    if not rows:
        return jsonify({"error": "no data in window"}), 404
    mat = np.concatenate([np.frombuffer(r[2], dtype=np.uint8).reshape(r[1], nfft)
                          for r in rows])                       # (cols, nfft)
    base = rows[0][0]
    c0 = max(c0, base); c1 = min(c1, base + mat.shape[0])
    mat = mat[c0 - base:c1 - base]
    df = fs / nfft                                    # row 0 (fftshifted) = fc - fs/2
    fi0 = max(0, int((f0 - fmin_full) / df))
    fi1 = min(nfft, max(fi0 + 1, int(math.ceil((f1 - fmin_full) / df))))
    band = mat[:, fi0:fi1].astype(np.float32).T       # (nbins, ncols) row0 = low f
    nbins = band.shape[0]
    fb = max(1, math.ceil(nbins / H))
    pad = (-nbins) % fb
    if pad:
        band = np.pad(band, ((0, pad), (0, 0)))
    img = band.reshape(band.shape[0] // fb, fb, -1).max(axis=1)
    img = qmin + (img / 255.0) * (qmax - qmin)        # uint8 -> dBm
    # locked scale: no drift on zoom
    vmin = _num("vmin", dvmin)
    vmax = _num("vmax", dvmax)
    return _tile(_colorize(img, vmin, vmax, request.args.get("cmap", "inferno")), {
        "t0": c0 * colw, "t1": c1 * colw,
        "fmin": fmin_full + fi0 * df, "fmax": fmin_full + fi1 * df,
        "cols": int(img.shape[1]), "nf": int(img.shape[0]), "level": level,
        "vmin": round(vmin, 1), "vmax": round(vmax, 1),
    })


if __name__ == "__main__":
    port = int(os.environ.get("SEA_PORT", "8090"))
    try:
        from waitress import serve           # production WSGI: keep-alive, faster
        print(f"serving on http://127.0.0.1:{port} (waitress)")
        serve(app, host="127.0.0.1", port=port, threads=8)
    except ImportError:
        app.run(host="127.0.0.1", port=port, debug=False, threaded=True)

#GDTBATH
