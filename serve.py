"""
serve.py  -  tiny Flask backend for the Tier 1 spectrum viewer.

Serves the viewer page and answers heatmap queries. For any requested time
window it auto-picks the coarsest pyramid level that still gives enough detail,
so panning/zooming across 2 years stays fast.

    cd C:\\Users\\pipyt\\spectrum-viewer
    py serve.py
    # open http://127.0.0.1:8000
"""

import os

import math
import threading

import duckdb
import numpy as np
from flask import Flask, jsonify, make_response, request, send_file

import tier2
from tier2 import bp as tier2_bp

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "spectrum.duckdb")

# Where the PSD / "PFP Aligned" folders live — the Box Drive mount.
# Override with the SEA_DATA_ROOT env var if your Box path differs.
DATA_ROOT = os.environ.get("SEA_DATA_ROOT", r"C:\Users\pipyt\Box\SEA-DATA")

# (table, bucket_seconds), finest first. None == raw native cadence.
LEVELS = [
    ("raw", 0),
    ("lvl_m10", 600),
    ("lvl_h1", 3600),
    ("lvl_h6", 21600),
    ("lvl_d1", 86400),
]

app = Flask(__name__)
# read_only so multiple requests share the file safely.
con = duckdb.connect(DB_PATH, read_only=True)

# Tier 2 drill-in (PSD / PFP) reads files on demand from DATA_ROOT.
app.config["DATA_ROOT"] = DATA_ROOT
app.config["SENSORS"] = [r[0] for r in con.execute(
    "SELECT sensor FROM meta ORDER BY sensor").fetchall()]

# Per-sensor available days, derived from the local summaries DB so the drill
# menu never has to enumerate the (huge, online-only) Box folder.
print("Indexing available days from summaries DB ...")
_days = {}
for sensor, d in con.execute(
        "SELECT sensor, CAST(to_timestamp(t) AS DATE) d FROM raw GROUP BY 1,2").fetchall():
    _days.setdefault(sensor, []).append(str(d))
for s in _days:
    _days[s].sort()
app.config["DAYS"] = _days
print(f"  {sum(len(v) for v in _days.values())} sensor-days indexed")

app.register_blueprint(tier2_bp)


def pick_level(span_seconds, target_cols):
    """Choose the finest level whose bucket count over this span <= target_cols."""
    for tbl, bucket in LEVELS:
        if bucket == 0:
            # raw: only use it when the window is short enough to be cheap
            if span_seconds <= 6 * 3600:
                return tbl, bucket
            continue
        if span_seconds / bucket <= target_cols:
            return tbl, bucket
    return LEVELS[-1]  # coarsest


@app.route("/")
def index():
    # never cache the page itself, so a refresh always gets the latest viewer.
    resp = make_response(send_file(os.path.join(HERE, "viewer.html")))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    return resp


@app.route("/api/meta")
def meta():
    rows = con.execute(
        "SELECT sensor, t_min, t_max, n FROM meta ORDER BY sensor").fetchall()
    sensors = [{"sensor": r[0], "t_min": r[1], "t_max": r[2], "n": r[3]}
               for r in rows]
    freqs = [r[0] for r in con.execute(
        "SELECT DISTINCT freq FROM raw ORDER BY freq").fetchall()]
    return jsonify({"sensors": sensors, "freqs": freqs})


@app.route("/api/heatmap")
def heatmap():
    sensor = request.args.get("sensor")
    t0 = float(request.args.get("t0"))
    t1 = float(request.args.get("t1"))
    width = int(request.args.get("width", 1600))
    span = max(t1 - t0, 1.0)

    tbl, bucket = pick_level(span, width)
    rows = con.execute(f"""
        SELECT freq, t, mx, md, mn
        FROM {tbl}
        WHERE sensor = ? AND t >= ? AND t < ?
        ORDER BY t
    """, [sensor, t0, t1]).fetchall()

    # Compact columnar payload: parallel arrays keep JSON small. Round the dBm
    # values to 0.1 and times to whole seconds so the JSON is ~half the size
    # (0.1 dBm is far finer than the colour resolution) — much faster to ship/parse.
    freq, t, mx, md, mn = [], [], [], [], []
    for r in rows:
        freq.append(r[0]); t.append(int(r[1]))
        mx.append(None if r[2] is None else round(r[2], 1))
        md.append(None if r[3] is None else round(r[3], 1))
        mn.append(None if r[4] is None else round(r[4], 1))
    return jsonify({
        "level": tbl, "bucket": bucket, "count": len(rows),
        "freq": freq, "t": t, "max": mx, "median": md, "mean": mn,
    })


# ---- PSD continuous layer (data-driven, crisp at any zoom) ------------
PSD_DB = os.path.join(HERE, "psd.duckdb")
F0 = 3530040000.0     # first PSD bin (Hz)
DF = 80000.0          # bin spacing (Hz)
NF = 2250             # bins  (full band 3530.04 .. 3709.96 MHz)
# PSD is stored as power-spectral-density (dBm/Hz); the summaries are channel
# power (dBm). Add 10*log10(channel bandwidth) to put the PSD layer on the SAME
# dBm scale as the summaries so the colours line up across the zoom boundary.
# 10 MHz channel -> +70 dB.  (An 80 kHz bin would be +49 dB; tune here if needed.)
PSD_DBM_OFFSET = 70.0


_psd_con = None
_psd_lock = threading.Lock()
# Per-sensor representative colour scale (vmin,vmax in dBm/Hz). Sampled once
# across the sensor's whole range so the day-view colours are STABLE — the same
# signal level always maps to the same colour no matter which window you first
# zoom into, so the detail view stays visually consistent with the summary.
_psd_scale_cache = {}


def _psd_scale(c, sensor, qmin, qmax):
    sc = _psd_scale_cache.get(sensor)
    if sc is not None:
        return sc
    rows = c.execute(
        "SELECT spec FROM psd WHERE sensor=? USING SAMPLE reservoir(400 ROWS)",
        [sensor]).fetchall()
    if not rows:
        sc = (qmin, qmax)
    else:
        specs = np.frombuffer(b"".join(r[0] for r in rows),
                              dtype=np.uint8).reshape(len(rows), NF)
        full = qmin + (specs.astype(np.float32) / 255.0) * (qmax - qmin) + PSD_DBM_OFFSET
        sc = (float(np.percentile(full, 2)), float(np.percentile(full, 98)))
    _psd_scale_cache[sensor] = sc
    return sc


def psd_conn():
    """Persistent read-only handle to the PSD DB (reused across requests so we
    don't pay the ~18ms reopen each zoom). The live DB isn't being written, so
    this stays valid; the master kills the server before swapping it."""
    global _psd_con
    if _psd_con is None:
        if not os.path.exists(PSD_DB):
            return None
        try:
            _psd_con = duckdb.connect(PSD_DB, read_only=True)
        except Exception:
            return None
    return _psd_con


@app.route("/api/psd_meta")
def psd_meta():
    sensor = request.args.get("sensor")
    c = psd_conn()
    if c is None:
        return jsonify({"has": False})
    with _psd_lock:
        row = c.execute(
            "SELECT f0, df, nf, t_min, t_max FROM psd_meta WHERE sensor=?",
            [sensor]).fetchone()
    if not row:
        return jsonify({"has": False})
    f0, df, nf, tmin, tmax = row
    return jsonify({"has": True, "fmin": f0, "fmax": f0 + (nf - 1) * df,
                    "t_min": tmin, "t_max": tmax})


@app.route("/api/psd_layer")
def psd_layer():
    sensor = request.args.get("sensor")
    t0 = float(request.args.get("t0")); t1 = float(request.args.get("t1"))
    W = max(1, int(request.args.get("w", 1200))); H = max(1, int(request.args.get("h", 600)))
    # optional frequency window (Hz); default = full band
    f0 = float(request.args.get("f0", 0)); f1 = float(request.args.get("f1", 0))
    fi0 = int(round((f0 - F0) / DF)) if f0 > 0 else 0
    fi1 = int(round((f1 - F0) / DF)) if f1 > 0 else NF - 1
    fi0 = max(0, min(NF - 1, fi0)); fi1 = max(0, min(NF - 1, fi1))
    if fi1 <= fi0:
        fi0, fi1 = 0, NF - 1
    c = psd_conn()
    if c is None:
        return jsonify({"error": "psd layer not ready"}), 503
    gap = False
    with _psd_lock:
        rows = c.execute(
            "SELECT t, spec FROM psd WHERE sensor=? AND t>=? AND t<? ORDER BY t",
            [sensor, t0, t1]).fetchall()
        if not rows:
            # Data gap: this window has no captures (common on sensors with
            # long offline stretches). Rather than a black plot, sample-and-hold
            # the most recent spectrum on either side of the gap so zooming in
            # keeps showing the last-known data (matches the summary layer's
            # gap-fill behaviour). The client labels it as a held/gap view.
            held = c.execute(
                "SELECT t, spec FROM psd WHERE sensor=? AND t<? ORDER BY t DESC LIMIT 1",
                [sensor, t1]).fetchone()
            if held is None:
                held = c.execute(
                    "SELECT t, spec FROM psd WHERE sensor=? AND t>=? ORDER BY t LIMIT 1",
                    [sensor, t1]).fetchone()
            if held is not None:
                rows = [held]
                gap = True
        mrow = c.execute("SELECT qmin, qmax FROM psd_meta WHERE sensor=?", [sensor]).fetchone()
    if not rows:
        return jsonify({"error": "no psd data for this sensor"}), 404
    qmin, qmax = mrow or (-180.0, -90.0)
    ncap = len(rows)
    # decode int8 spectra: (ncap, NF). One row per capture (time = native cadence).
    specs = np.frombuffer(b"".join(r[1] for r in rows), dtype=np.uint8).reshape(ncap, NF)
    band = specs[:, fi0:fi1 + 1].astype(np.float32)   # (ncap, nbins) over requested freq band
    nbins = band.shape[1]
    fb = max(1, math.ceil(nbins / H))                 # freq bins per pixel row
    pad = (-nbins) % fb
    if pad:
        band = np.pad(band, ((0, 0), (0, pad)))       # pad high-freq end (max-binning ignores 0)
    nF = band.shape[1] // fb
    binned = band.reshape(ncap, nF, fb).max(axis=2)   # max over each freq pixel
    img = qmin + (binned.T / 255.0) * (qmax - qmin) + PSD_DBM_OFFSET   # (nF, ncap) dBm, row 0 = low freq (top)
    # Colour scale: a locked vmin/vmax from the client keeps colours stable across
    # zoom; otherwise derive it from the FULL spectrum (not the visible band) so
    # zooming frequency never remaps colours.
    qv0 = request.args.get("vmin"); qv1 = request.args.get("vmax")
    if qv0 is not None and qv1 is not None:
        vmin, vmax = float(qv0), float(qv1)
    else:
        # Stable per-sensor scale (sampled across the whole range), not just this
        # window, so colours stay consistent across zoom/pan and match the summary.
        with _psd_lock:
            vmin, vmax = _psd_scale(c, sensor, qmin, qmax)
    rgb = tier2._colorize(img, vmin, vmax, request.args.get("cmap", "inferno"))
    png = tier2._encode(rgb)
    return jsonify({
        "png": tier2._b64(png),
        "t0": float(t0), "t1": float(t1),
        "fmin": F0 + fi0 * DF, "fmax": F0 + fi1 * DF,
        "ncap": int(ncap), "cols": int(ncap), "nf": int(nF),
        "vmin": round(vmin, 1), "vmax": round(vmax, 1),
        "gap": gap,
    })


# ---- PFP layer (periodic-frame-power, ~18 us within a 10 ms frame) ----
PFP_DB = os.path.join(HERE, "pfp.duckdb")


_pfp_con = None
_pfp_lock = threading.Lock()


def pfp_conn():
    global _pfp_con
    if _pfp_con is None:
        if not os.path.exists(PFP_DB):
            return None
        try:
            _pfp_con = duckdb.connect(PFP_DB, read_only=True)
        except Exception:
            return None
    return _pfp_con


_pfp_freqs_cache = {}   # sensor -> channel freqs (static; the DISTINCT scan over the
                        # 14 GB table costs ~270 ms, so cache it after the first call)


@app.route("/api/pfp_meta")
def pfp_meta():
    sensor = request.args.get("sensor")
    c = pfp_conn()
    if c is None:
        return jsonify({"has": False})
    with _pfp_lock:
        m = c.execute("SELECT npos, frame_ms, t_min, t_max, stat FROM pfp_meta WHERE sensor=?",
                      [sensor]).fetchone()
        if not m:
            return jsonify({"has": False})
        freqs = _pfp_freqs_cache.get(sensor)
        if freqs is None:
            freqs = [r[0] for r in c.execute(
                "SELECT DISTINCT freq FROM pfp WHERE sensor=? ORDER BY 1", [sensor]).fetchall()]
            _pfp_freqs_cache[sensor] = freqs
    return jsonify({"has": True, "npos": m[0], "frame_ms": m[1],
                    "t_min": m[2], "t_max": m[3], "stat": m[4], "freqs": freqs})


@app.route("/api/pfp_frame")
def pfp_frame():
    """Frame heatmap for one channel: X = frame position (0..10 ms),
    Y = capture time over [t0,t1]. Rendered from int8 frames."""
    sensor = request.args.get("sensor")
    freq = float(request.args.get("freq"))
    t0 = float(request.args.get("t0")); t1 = float(request.args.get("t1"))
    H = max(1, int(request.args.get("h", 600)))
    c = pfp_conn()
    if c is None:
        return jsonify({"error": "pfp not ready"}), 503
    with _pfp_lock:
        rows = c.execute(
            "SELECT t, frame FROM pfp WHERE sensor=? AND freq=? AND t>=? AND t<? ORDER BY t",
            [sensor, freq, t0, t1]).fetchall()
        m = c.execute("SELECT npos, frame_ms, qmin, qmax FROM pfp_meta WHERE sensor=?",
                      [sensor]).fetchone()
    if not rows:
        return jsonify({"error": "no pfp in window"}), 404
    npos, frame_ms, qmin, qmax = m
    ncap = len(rows)
    times = [r[0] for r in rows]
    frames = np.frombuffer(b"".join(r[1] for r in rows), dtype=np.uint8).reshape(ncap, npos)
    # X = capture time (cols, continuous with the rest), Y = frame position (rows).
    mat = frames.T.astype(np.float32)                 # (npos, ncap)
    nrows = min(H, npos)
    if npos > nrows:
        fb = math.ceil(npos / nrows)
        pad = (-npos) % fb
        m2 = np.pad(mat, ((0, pad), (0, 0))) if pad else mat
        mat = m2.reshape(m2.shape[0] // fb, fb, ncap).max(axis=1)
    img = qmin + (mat / 255.0) * (qmax - qmin)        # (nrows, ncap) dBm; row 0 = frame start (top)
    qv0 = request.args.get("vmin"); qv1 = request.args.get("vmax")
    if qv0 is not None and qv1 is not None:
        vmin, vmax = float(qv0), float(qv1)
    else:
        vmin = float(np.percentile(img, 2)); vmax = float(np.percentile(img, 98))
    rgb = tier2._colorize(img, vmin, vmax, request.args.get("cmap", "inferno"))
    png = tier2._encode(rgb)
    return jsonify({
        "png": tier2._b64(png),
        "t0": float(times[0]), "t1": float(times[-1]), "ncap": ncap,
        "npos": npos, "nrows": int(img.shape[0]), "frame_ms": frame_ms, "freq": freq,
        "vmin": round(vmin, 1), "vmax": round(vmax, 1),
    })


# ---- IQ capture mode (independent SigMF/TDMS captures; own axes) ------
# Renders come from the iq.duckdb STFT pyramid built by iq_ingest.py.
# Thread-safe: every request opens its own read-only connection (renders can
# run in parallel; iq_ingest may also be appending to the DB).
IQ_DB = os.environ.get("IQ_DB", os.path.join(HERE, "iq.duckdb"))


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
    t0 = max(0.0, float(request.args.get("t0", 0)))
    t1 = min(dur, float(request.args.get("t1", dur)))
    W = max(1, int(request.args.get("w", 1200)))
    H = max(1, int(request.args.get("h", 600)))
    fmin_full, fmax_full = fc - fs / 2, fc + fs / 2
    f0 = max(fmin_full, float(request.args.get("f0", fmin_full)))
    f1 = min(fmax_full, float(request.args.get("f1", fmax_full)))
    if t1 <= t0 or f1 <= f0:
        c.close()
        return jsonify({"error": "empty window"}), 400
    # finest level whose column count over this span <= 2*w (like pick_level)
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
    # frequency slice: row 0 of the fftshifted STFT = fc - fs/2 (low freq)
    df = fs / nfft
    fi0 = max(0, int((f0 - fmin_full) / df))
    fi1 = min(nfft, max(fi0 + 1, int(math.ceil((f1 - fmin_full) / df))))
    band = mat[:, fi0:fi1].astype(np.float32).T       # (nbins, ncols) row0 = low f
    nbins = band.shape[0]
    fb = max(1, math.ceil(nbins / H))                 # bin freq down to <= H rows
    pad = (-nbins) % fb
    if pad:
        band = np.pad(band, ((0, pad), (0, 0)))
    img = band.reshape(band.shape[0] // fb, fb, -1).max(axis=1)
    img = qmin + (img / 255.0) * (qmax - qmin)        # uint8 -> dBm
    qv0, qv1 = request.args.get("vmin"), request.args.get("vmax")
    vmin = float(qv0) if qv0 is not None else dvmin   # locked scale: no drift on zoom
    vmax = float(qv1) if qv1 is not None else dvmax
    rgb = tier2._colorize(img, vmin, vmax, request.args.get("cmap", "inferno"))
    return jsonify({
        "png": tier2._b64(tier2._encode(rgb)),
        "t0": c0 * colw, "t1": c1 * colw,
        "fmin": fmin_full + fi0 * df, "fmax": fmin_full + fi1 * df,
        "cols": int(img.shape[1]), "nf": int(img.shape[0]), "level": level,
        "vmin": round(vmin, 1), "vmax": round(vmax, 1),
    })


if __name__ == "__main__":
    # launched via .claude/launch.json ("spectrum") -> port 8090
    port = int(os.environ.get("SEA_PORT", "8000"))
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
