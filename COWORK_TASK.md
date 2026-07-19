# TASK: Add an "IQ Capture" spectrogram mode to the existing spectrum viewer

## Where the project lives
`C:\Users\pipyt\spectrum-viewer` — a working RF spectrogram viewer.
- **Backend:** `serve.py` (Flask, port 8090, launched via `py serve.py`; `.claude/launch.json` defines it as "spectrum").
- **Frontend:** `viewer.html` (single-file HTML5 canvas app, ~700 lines).
- **Render helpers:** `tier2.py` (turbo colormap `_colorize`, WebP encode `_encode(quality=80, method=4)`, base64 `_b64`; renders use **PER-CALL** `duckdb.connect()` for thread-safety — never a shared connection).
- **Ingest scripts:** `psd_ingest.py` / `pfp_ingest.py` (resumable, quantize float dB to int8 BLOBs into DuckDB). **Study these — they are the pattern to copy.**
- The current viewer visualizes NIST NASCTN CBRS SEA-DATA across 10 sensors and ~2 years. It has THREE continuous layers on one canvas (X=time always): summary → PSD (2250 bins @ 80 kHz) → PFP frame. Frequency is a vertical slider on the left. Colors are locked per sensor/channel so they DON'T drift on zoom. Rendering uses `ctx.imageSmoothingEnabled = false` (sharp nearest-neighbor — do NOT re-enable smoothing).
- **Existing endpoints:** `/api/meta`, `/api/heatmap`, `/api/psd_meta`, `/api/psd_layer`, `/api/pfp_meta`, `/api/pfp_frame`. **Existing client constants:** `layerMode` (`'summary'|'psd'|'pfp'`), `FULL_F0`/`FULL_F1` (3530.04–3709.96 MHz), `viewF`, `PSD_THRESHOLD`, `PFP_TIME_THRESHOLD`, `drawCrisp()`, `requestData()`.

## The new data (NIST Applied Systems Metrology Group — I/Q Data Sets)
Source index: https://www.nist.gov/ctl/spectrum-technology-and-research-division/applied-systems-metrology-group/iq-data-sets

Four datasets, each a DOI that 302-redirects to `data.nist.gov/od/id/<id>` (the landing pages are JS SPAs — get files via the PDR distribution API at `https://data.nist.gov/rmm/records/<id>` or the "Access data" bundle link, **NOT** by scraping the HTML):
1. **Field Captures of 4G FDD LTE Uplink through a Telemetry Antenna** — `doi.org/10.18434/mds2-2413` (NTIA TR-21-553)
2. **Laboratory captures of 4G FDD LTE Uplink** — `doi.org/10.18434/mds2-2395` (NIST TN 2159)
3. **Wi-Fi and Bluetooth recordings, 2.4/5 GHz** — `doi.org/10.18434/mds2-2731` (NIST TN 2237)
4. **High-SNR FDD LTE Uplink from a COTS handset** — `doi.org/10.18434/mds2-3177` (NIST TN 2286)

These are **SigMF**: a `.sigmf-meta` JSON sidecar + a `.sigmf-data` raw binary of interleaved I/Q. **CRITICAL:** sample rate, center frequency, and datatype VARY per capture and per dataset — you MUST read them from the metadata, never hardcode. Expect fields: `global["core:sample_rate"]`, `global["core:datatype"]` (e.g. `cf32_le`, `ci16_le`, `ci16_be`), and center frequency in `captures[0]["core:frequency"]` OR an ntia extension. Durations range from a few ms to ~1 s; at 30.72 Msps a 1 s capture is ~250 MB (cf32) — so these are big and MUST be tiled/pyramided, not sent raw.

## THE KEY DESIGN DECISION (do this, don't fight it)
IQ captures do NOT fit the CBRS time-continuous, multi-sensor, fixed-band model. Do NOT try to jam them onto the existing global time axis or the 3530–3710 MHz frequency range. Instead add a NEW top-level MODE: an **"IQ Library"** of independent captures. When the user selects a capture, the SAME canvas/zoom/colormap machinery renders a spectrogram of THAT capture with its OWN coordinate system:
- **X axis** = elapsed time WITHIN the capture (0 … duration).
- **Y axis** = frequency, centered on the capture's own `fc`, spanning `fc ± sample_rate/2`.
- Continuous zoom in both axes, from whole-capture overview down to fine STFT detail — reuse the existing pyramid-picking + preview-stretch + `drawCrisp` logic.

A source/mode switcher (dropdown in the header) toggles between "CBRS SEA monitoring" (existing, untouched) and "IQ capture: `<name>`". Keep the two modes cleanly separated so nothing about the CBRS path regresses.

## What to build
1. **SigMF data handler** (new module, e.g. `sigmf_io.py`): given a `.sigmf-meta` path, parse JSON → return `{sample_rate, center_freq, datatype, n_samples, duration, annotations}`. Map SigMF datatype strings to numpy dtypes (`cf32_le`→complex64, `ci16_le`→int16 I/Q interleaved little-endian→complex, etc.) and provide a reader that memory-maps the `.sigmf-data` and yields complex samples for a requested `[start_sample, end_sample)` slice. Fall back gracefully if a dataset ships `.mat`/`.npy` instead — detect and branch. Validate inputs at the boundary (dtype known, file size matches n_samples).
2. **A good specgram function**: STFT via numpy (`rfft`) or `scipy.signal` — configurable `nfft`, window (Hann), overlap. Output power in dB (`10*log10|X|^2`), `fftshift` so DC is centered, so Y maps to real RF frequency (`fc-fs/2 … fc+fs/2`). Must handle captures far longer than memory by streaming/averaging blocks.
3. **A precompute/ingest step** (`iq_ingest.py`, resumable like `psd_ingest.py`): for each capture, compute a MULTI-RESOLUTION spectrogram pyramid and store as int8 BLOBs in a DuckDB (`iq.duckdb`) — quantize dB to int8 with fixed QMIN/QMAX per capture, store `vmin`/`vmax` so colors lock and DON'T drift on zoom. Coarsest level = whole capture at low res; finest = full STFT resolution. Small-file discipline (int8, WebP) exactly like the PSD path. An `iq_meta` table lists captures: `{id, dataset, name, fc, fs, duration, nfreq, ntime_levels, vmin, vmax}`.
4. **New Flask endpoints** in `serve.py` (thread-safe, per-call `duckdb.connect`):
   - `/api/iq_index` → list of available captures (id, name, dataset, fc, fs, duration).
   - `/api/iq_meta?id=…` → per-capture axes/scale info.
   - `/api/iq_layer?id=…&t0=…&t1=…&f0=…&f1=…&w=…&h=…` → returns a WebP tile (turbo-colorized via `tier2._colorize`/`_encode`) for the requested time/freq window, auto-picking the right pyramid level (mirror `pick_level()` in `serve.py`). Pass `&vmin&vmax` through so colors stay locked across zoom.
5. **Viewer integration** in `viewer.html`: add the mode switcher + a capture picker; add `layerMode 'iq'`; a per-capture coordinate transform (time-in-capture ↔ X, RF-freq ↔ Y using the capture's `fc`/`fs`, replacing the CBRS `FULL_F0`/`FULL_F1` globals when in IQ mode); wire `requestData()`/`drawCrisp()` to fetch and draw `iq_layer` tiles with `imageSmoothingEnabled=false` and the stretch-while-loading preview. Readout shows time (µs/ms), RF frequency (MHz), and power (dBm).

## Constraints (from the project's CLAUDE.md and prior decisions — honor them)
- Prefer EDITING existing files (`serve.py`, `viewer.html`, `tier2.py`) over creating new ones; only add new modules where a genuinely new concern (`sigmf_io`, `iq_ingest`). Keep every file under 500 lines.
- Do NOT regress the CBRS mode. It must look and behave exactly as it does now.
- Sharp rendering (`imageSmoothingEnabled=false`). Stable colors across zoom (locked `vmin`/`vmax`). Small files (int8 + WebP method=4).
- Thread-safe renders: per-call `duckdb.connect()`, never a shared write/read connection during rendering.
- Server on port 8090. Never commit secrets/.env. Don't add `Co-Authored-By` trailers.

## Deliverable & acceptance
Start with ONE dataset end-to-end — recommend the LTE field captures (`mds2-2413`) or Wi-Fi/BT (`mds2-2731`) — download it, run `iq_ingest.py` to build `iq.duckdb`, then VERIFY in the browser: open http://127.0.0.1:8090, switch to IQ mode, pick a real capture, and confirm a correct STFT spectrogram renders with LTE resource-block / Wi-Fi burst structure visible, continuous zoom works in both time and frequency, colors stay stable on zoom, and the CBRS mode is unaffected. Take a screenshot as proof. Then generalize the handler so the other three datasets ingest by pointing the script at their folders. Log any dataset whose format needs a special case.

## Gotchas learned on this project
- data.nist.gov DOIs redirect and the landing pages are JS SPAs — use the PDR record/distribution API for file listings and direct download URLs, don't scrape HTML.
- SigMF datatype and sample_rate/center_freq MUST come from the meta file; different captures differ. `fftshift` is required so Y maps to true RF frequency.
- IQ files are large (Msps × seconds). Never load a whole capture into RAM or send raw to the browser — stream, pyramid, quantize.
- PowerShell mangles inline Python containing `%` or `$_` — use a heredoc or a `.py` file.
- Detached background work survives session boundaries only if launched with `Start-Process ... -WindowStyle Hidden` (not `run_in_background`). Downloads/ingest of big datasets should run detached and resumable.

## Reference
- NTIA SigMF namespace extensions: https://github.com/NTIA/sigmf-ns-ntia
- NIST I/Q Data Sets index: https://www.nist.gov/ctl/spectrum-technology-and-research-division/applied-systems-metrology-group/iq-data-sets
