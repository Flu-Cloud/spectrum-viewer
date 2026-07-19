# Spectrum Viewer

A "Google-Maps for RF spectrum" — a pan/zoom time × frequency power heatmap over the
[NIST NASCTN CBRS SEA](https://www.nist.gov/programs-projects/spectrum-monitoring-cbrs-band)
sensor dataset (10 sensors, 3.5 GHz CBRS band, 18 channels).

One continuous canvas where **X is always time**. As you zoom, the viewer swaps between
three resolution layers automatically:

| Layer | Source | Resolution | Shown when |
|-------|--------|------------|------------|
| **Summary** | `spectrum.duckdb` | ~90 s sweeps, 18 channels | zoomed out (months → days) |
| **PSD** | `psd.duckdb` | 2250 freq bins @ 80 kHz, ~4-min captures | time span ≤ 3 days |
| **PFP** | `pfp.duckdb` | 560 pts across a 10 ms frame (~18 µs/pt) | freq narrowed to ~1 channel, or span ≤ 30 min |

Colors are locked per sensor so they stay stable across zoom. Frequency zoom is the
vertical slider on the left (drag up = zoom in); drag the heatmap vertically to pan
frequency.

There is also an independent **IQ capture mode** (header "Source" dropdown): STFT
spectrograms of NIST I/Q datasets (SigMF / TDMS), each with its own time/frequency
axes, backed by a multi-level pyramid in `iq.duckdb` (see `IQ_DATASETS.md`).

## Architecture

- **`serve.py`** — the whole backend (Flask). Serves `viewer.html` and all APIs
  (`/api/heatmap`, `/api/psd_layer`, `/api/pfp_frame`, `/api/iq_*`, plus `*_meta`
  availability endpoints), auto-picking the coarsest stored level that still has
  detail for the requested window, and rendering WebP tiles (numpy + Pillow;
  metadata rides in the `X-Meta` response header). Reads the DuckDB files read-only.
- **`viewer.html`** — single-file canvas app (no build step). All zoom/pan, the
  layer swaps, the zoom sliders, and color locking live here.
- **`sigmf_io.py`** — unified reader for IQ capture formats (SigMF/TDMS/npy).

### Data storage

Quantized **int8** spectra/frames, grouped into **zlib-compressed chunks** of
consecutive captures (`psd_chunk`: 256 spectra, `pfp_chunk`: 1024 frames — consecutive
same-channel frames compress ~3×). Summary tables store dBm as `SMALLINT` (dBm×10).
Built from the source CSVs by the ingest scripts + `compact_db.py`; **not** checked
into git (multi-GB).

## Ingest scripts

These read the source dataset (CSV products on a Box Drive mount) and write the compact
DuckDB files. All are per-sensor and resumable.

| Script | Builds | Notes |
|--------|--------|-------|
| `build_db.py` | `spectrum.duckdb` | summaries + pyramid levels (lvl_m10/h1/h6/d1) |
| `psd_ingest.py` | `psd.duckdb` | PSD `max`, int8 BLOB per capture (old row schema) |
| `pfp_ingest.py` | `pfp.duckdb` | PFP `max_peak`, int8 BLOB per channel/capture (old row schema) |
| `master_ingest.py` | both build DBs | ingests all 10 sensors, then swaps build → live |
| `compact_db.py` | `*_c.duckdb` | **run after any ingest**: repacks into the chunked/zlib schema the server reads, then swap `*_c` → live |
| `iq_ingest.py` | `iq.duckdb` | STFT pyramid for IQ captures (SigMF/TDMS/npy) |
| `run_master.ps1` | — | runs `master_ingest.py` detached with a crash-retry loop |

## Running

Requires Python 3 with the deps in `requirements.txt`.

```bash
pip install -r requirements.txt

# point at your copy of the source data (only needed for ingest / on-demand drill)
export SEA_DATA_ROOT="/path/to/SEA-DATA"   # PowerShell: $env:SEA_DATA_ROOT="..."

# choose a port (default 8090)
export SEA_PORT=8090                        # PowerShell: $env:SEA_PORT="8090"

python serve.py
# open http://localhost:8090
```

The DuckDB files must be present beside `serve.py` for the viewer to show data. Build
them with the ingest scripts above, or copy existing ones in.

## Data

This repo contains **only code** — no spectrum data. The underlying measurements are the
NIST NASCTN CBRS SEA dataset (public). The ingest scripts expect the CSV products
(`Summaries`, `PSD`, `PFP Aligned`) laid out under `SEA_DATA_ROOT`.

## Hosting notes

The viewer is currently a local Flask app. To host it live (e.g. behind Django or another
framework) the main consideration is the multi-GB DuckDB files: they need to live on the
server's disk or in object storage next to the app, not in the repo. The Flask app is
stateless and read-only against those files, so it ports cleanly to any WSGI host.
