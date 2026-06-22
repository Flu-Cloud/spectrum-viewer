"""
tier2.py  -  drill-in (PSD day spectrogram + PFP frame view).

Rendering is split into pure functions (render_psd_file / render_pfp_file) that
turn a CSV day-file into PNG bytes + axis metadata. Both the live endpoints and
the offline pre-render batch (prerender.py) call these.

Tiles: once rendered, a PNG + JSON is written under TILE_ROOT. After that the day
opens instantly (just read the tile). If a tile is missing the endpoint renders
on demand and writes the tile, so browsing also warms the cache.

Layout under DATA_ROOT (mirrors Box "SEA-DATA"):
    PSD/<date>_<sensor>_<stat>.csv
    PFP Aligned/PFP_<date>_<sensor>_<stat>.csv
"""

import base64
import glob
import io
import json
import os

import duckdb
import numpy as np
from flask import Blueprint, current_app, jsonify, request
from PIL import Image

bp = Blueprint("tier2", __name__)
_con = duckdb.connect()

HERE = os.path.dirname(os.path.abspath(__file__))
TILE_ROOT = os.path.join(HERE, "tiles")


# ---- Turbo colormap LUT ----------------------------------------------
def _turbo_lut():
    x = np.linspace(0, 1, 256)
    r = 34.61 + x*(1172.33 + x*(-10793.56 + x*(33300.12 + x*(-38394.49 + x*14825.05))))
    g = 23.31 + x*(557.33 + x*(1225.33 + x*(-3574.96 + x*(3520.99 + x*-1300.91))))
    b = 27.2 + x*(3211.1 + x*(-15327.97 + x*(27814.0 + x*(-22569.18 + x*6838.66))))
    return np.clip(np.stack([r, g, b], axis=1), 0, 255).astype(np.uint8)

LUT = _turbo_lut()


def _colorize(mat, vmin, vmax):
    norm = (mat - vmin) / max(vmax - vmin, 1e-6)
    idx = np.clip(norm * 255, 0, 255)
    nan = ~np.isfinite(mat)
    idx[nan] = 0
    rgb = LUT[idx.astype(np.uint8)]
    rgb[nan] = (8, 9, 12)
    return rgb


# WebP q80 is visually identical to lossless for these spectrograms (only the
# noise floor is smoothed) at ~1/10 the size. The CSVs remain the source of truth.
TILE_QUALITY = 80


def _encode(rgb):
    buf = io.BytesIO()
    # method=4 encodes ~7x faster than 6 for ~10% larger output (visually identical) —
    # these are rendered live per zoom, so encode speed dominates the felt latency.
    Image.fromarray(rgb, "RGB").save(buf, format="WEBP", quality=TILE_QUALITY, method=4)
    return buf.getvalue()


def _b64(img):
    return "data:image/webp;base64," + base64.b64encode(img).decode()


# ---- Pure renderers (used by endpoints + prerender) -------------------
def render_psd_file(path):
    """CSV -> (png_bytes, meta). Image: X=time, Y=frequency (high at top)."""
    con = duckdb.connect()      # per-call connection: safe for parallel use
    rel = con.execute("SELECT * FROM read_csv_auto(?, header=true)", [path])
    cols = [d[0] for d in rel.description]
    rows = rel.fetchall()
    if not rows or len(cols) < 2:
        raise ValueError("empty PSD file")
    freqs = np.array([float(c) for c in cols[1:]])
    times = [r[0].timestamp() for r in rows]
    mat = np.array([[np.nan if v is None else v for v in r[1:]] for r in rows],
                   dtype=np.float64)
    img = mat.T[::-1, :]
    finite = img[np.isfinite(img)]
    vmin = float(np.percentile(finite, 2)) if finite.size else -130.0
    vmax = float(np.percentile(finite, 98)) if finite.size else -50.0
    meta = {"t0": times[0], "t1": times[-1], "nt": len(times),
            "fmin": float(freqs.min()), "fmax": float(freqs.max()), "nf": int(freqs.size),
            "vmin": round(vmin, 1), "vmax": round(vmax, 1)}
    return _encode(_colorize(img, vmin, vmax)), meta


def render_pfp_file(path):
    """CSV -> (meta, {freq: png_bytes}). One image per 10 MHz channel.
    Image: X=frame position (0..10ms), Y=capture time (newest top)."""
    con = duckdb.connect()      # per-call connection: safe for parallel use
    rel = con.execute("SELECT * FROM read_csv_auto(?, header=true)", [path])
    cols = [d[0] for d in rel.description]            # datetime, frequency, 0..N
    rows = rel.fetchall()
    npos = len(cols) - 2
    freqs = sorted({r[1] for r in rows})
    pngs, chans = {}, {}
    for f in freqs:
        sel = [r for r in rows if r[1] == f]
        times = [r[0].timestamp() for r in sel]
        mat = np.array([[np.nan if v is None else v for v in r[2:]] for r in sel],
                       dtype=np.float64)[::-1, :]
        finite = mat[np.isfinite(mat)]
        vmin = float(np.percentile(finite, 2)) if finite.size else -120.0
        vmax = float(np.percentile(finite, 98)) if finite.size else -40.0
        pngs[f] = _encode(_colorize(mat, vmin, vmax))
        chans[str(f)] = {"t0": times[0], "t1": times[-1], "nt": len(times),
                         "vmin": round(vmin, 1), "vmax": round(vmax, 1)}
    meta = {"npos": npos, "frame_ms": 10.0, "freqs": freqs, "channels": chans}
    return meta, pngs


# ---- Tile cache -------------------------------------------------------
def psd_tile(sensor, date, stat):
    base = os.path.join(TILE_ROOT, "PSD", sensor, f"{date}_{stat}")
    return base + ".webp", base + ".json"


def pfp_tile(sensor, date, stat, freq=None):
    d = os.path.join(TILE_ROOT, "PFP", sensor, f"{date}_{stat}")
    if freq is None:
        return d + ".json"
    return d + f"_{int(round(freq/1e6))}.webp"


def save_psd_tile(sensor, date, stat, png, meta):
    p_png, p_json = psd_tile(sensor, date, stat)
    os.makedirs(os.path.dirname(p_png), exist_ok=True)
    with open(p_png, "wb") as f:
        f.write(png)
    with open(p_json, "w") as f:
        json.dump(meta, f)


def save_pfp_tiles(sensor, date, stat, meta, pngs):
    p_json = pfp_tile(sensor, date, stat)
    os.makedirs(os.path.dirname(p_json), exist_ok=True)
    for fr, png in pngs.items():
        with open(pfp_tile(sensor, date, stat, fr), "wb") as f:
            f.write(png)
    with open(p_json, "w") as f:
        json.dump(meta, f)


# ---- File discovery ---------------------------------------------------
def _root():
    return current_app.config["DATA_ROOT"]


def _sensors():
    return current_app.config["SENSORS"]


def _parse(fname, sensors):
    n = os.path.basename(fname)
    if not n.lower().endswith(".csv"):
        return None
    n = n[:-4]
    if n.startswith("PFP_"):
        n = n[4:]
    if len(n) < 11 or n[10] != "_":
        return None
    date, rest = n[:10], n[11:]
    for s in sensors:
        if rest.startswith(s + "_"):
            return date, s, rest[len(s) + 1:]
    return None


_scan_cache = {}   # (root, subdir) -> (timestamp, result); avoids re-globbing Box


def scan(root, subdir, sensors, ttl=300):
    import time
    key = (root, subdir)
    hit = _scan_cache.get(key)
    if hit and (time.time() - hit[0]) < ttl:
        return hit[1]
    out = {}
    for f in glob.glob(os.path.join(root, subdir, "*.csv")):
        p = _parse(f, sensors)
        if p:
            date, sensor, stat = p
            out.setdefault(sensor, {}).setdefault(date, []).append(stat)
    _scan_cache[key] = (time.time(), out)
    return out


def _find_in(root, subdir, sensor, date, stat, prefix=""):
    hits = glob.glob(os.path.join(root, subdir, f"{prefix}{date}_{sensor}_{stat}.csv"))
    return hits[0] if hits else None


def _find(subdir, sensor, date, stat, prefix=""):
    return _find_in(_root(), subdir, sensor, date, stat, prefix)


# ---- Endpoints --------------------------------------------------------
@bp.route("/api/drill/index")
def drill_index():
    sensor = request.args.get("sensor")
    # Days come from the local summaries DB (instant) — no Box enumeration.
    dates = current_app.config["DAYS"].get(sensor, [])
    return jsonify({
        "psd": {"dates": dates, "stats": ["max"]},
        "pfp": {"dates": [], "stats": []},
    })


@bp.route("/api/psd")
def psd():
    sensor, date = request.args.get("sensor"), request.args.get("date")
    stat = request.args.get("stat", "max")
    p_png, p_json = psd_tile(sensor, date, stat)
    if os.path.exists(p_png) and os.path.exists(p_json):
        meta = json.load(open(p_json))
        png = open(p_png, "rb").read()
    else:
        path = _find("PSD", sensor, date, stat)
        if not path:
            return jsonify({"error": f"no PSD file for {sensor} {date} {stat}"}), 404
        png, meta = render_psd_file(path)
        try:
            save_psd_tile(sensor, date, stat, png, meta)
        except OSError:
            pass
    out = dict(meta); out["png"] = _b64(png); out["stat"] = stat
    return jsonify(out)


@bp.route("/api/pfp")
def pfp():
    sensor, date = request.args.get("sensor"), request.args.get("date")
    stat, freq = request.args.get("stat"), request.args.get("freq")
    if not stat:
        idx = scan(_root(), "PFP Aligned", _sensors()).get(sensor, {}).get(date, [])
        # prefer a cached tile-stat if present
        cached = glob.glob(os.path.join(TILE_ROOT, "PFP", sensor, f"{date}_*.json"))
        if cached:
            stat = os.path.basename(cached[0])[len(date)+1:-5]
        elif idx:
            stat = sorted(idx)[0]
        else:
            return jsonify({"error": "no PFP for this sensor/date"}), 404

    p_json = pfp_tile(sensor, date, stat)
    meta = None
    if os.path.exists(p_json):
        meta = json.load(open(p_json))
    if meta is None:
        path = _find("PFP Aligned", sensor, date, stat, prefix="PFP_")
        if not path:
            return jsonify({"error": f"no PFP file for {sensor} {date} {stat}"}), 404
        meta, pngs = render_pfp_file(path)
        try:
            save_pfp_tiles(sensor, date, stat, meta, pngs)
        except OSError:
            pass
        else:
            pngs = None  # now on disk

    if freq is None:
        return jsonify({"freqs": meta["freqs"], "stat": stat})

    f = float(freq)
    tile = pfp_tile(sensor, date, stat, f)
    if os.path.exists(tile):
        png = open(tile, "rb").read()
    else:
        # render just-in-time (tile write failed or not yet saved)
        path = _find("PFP Aligned", sensor, date, stat, prefix="PFP_")
        _, pngs = render_pfp_file(path)
        png = pngs.get(f)
        if png is None:
            return jsonify({"error": f"freq {f} not in file"}), 404
    ch = meta["channels"].get(str(f), {})
    return jsonify({
        "png": _b64(png), "freq": f, "stat": stat,
        "npos": meta["npos"], "frame_ms": meta["frame_ms"], "freqs": meta["freqs"],
        "t0": ch.get("t0"), "t1": ch.get("t1"), "nt": ch.get("nt"),
        "vmin": ch.get("vmin"), "vmax": ch.get("vmax"),
    })
