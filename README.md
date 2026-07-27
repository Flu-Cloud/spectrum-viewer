# Spectrum Viewer

A "Google Maps for RF spectrum": one pan/zoom canvas where **X is always
time** and colour is power. Zoom from a two-year overview down to the
microsecond structure of a single burst. The viewer swaps resolution layers
automatically as you go.

![CBRS spectrum overview](docs/cbrs_overview.jpg)

It draws two independent data sources on the same machinery:

- **CBRS SEA monitoring**: the [NIST NASCTN CBRS SEA](https://www.nist.gov/programs-projects/spectrum-monitoring-cbrs-band)
  dataset (10 sensors, 3.5 GHz band, ~2 years), as one continuous time axis.
- **IQ captures**: STFT spectrograms of individual [NIST I/Q recordings](https://www.nist.gov/ctl/spectrum-technology-and-research-division/applied-systems-metrology-group/iq-data-sets)
  (SigMF / TDMS), each with its own time and frequency axes.

# Quick start

Requires Python 3.10+ and the dependencies in `requirements.txt`. The real
datasets are multi-GB and aren't in this repo, so the quickest way to see the
viewer work is the built-in synthetic demo. **No downloads required**:

```bash
pip install -r requirements.txt
python examples/make_sample.py     # fabricate a small capture + build iq.duckdb
python serve.py                    # open http://127.0.0.1:8090
```

Then open the **Source** dropdown in the header and pick
**"IQ capture: demo_signal"**. See [examples/](examples/) for what to look for.

![Synthetic demo spectrogram](docs/demo_spectrogram.png)

# Use case

Spectrum measurements are awkward to explore. A monitoring campaign spans years
of ~90-second sweeps, while a single I/Q capture is milliseconds of data at tens
of megasamples per second. Spectrum Viewer puts both on the same interface. One
canvas, continuous zoom, and colours that stay **locked** per sensor/capture, so
a given power level always maps to the same colour no matter how far you zoom in.

## CBRS monitoring mode

One continuous canvas where **X is always time**. As you zoom in, the viewer
swaps between three stored-resolution layers automatically:

| Layer | Source | Resolution | Shown when |
|-------|--------|------------|------------|
| **Summary** | `spectrum.duckdb` | ~90 s sweeps, 18 channels | zoomed out (months → days) |
| **PSD** | `psd.duckdb` | 2250 freq bins @ 80 kHz, ~4-min captures | time span ≤ 3 days |
| **PFP** | `pfp.duckdb` | 560 pts across a 10 ms frame (~18 µs/pt) | freq narrowed to ~1 channel, or span ≤ 30 min |

Frequency zoom is the vertical slider on the left (drag up = zoom in); drag the
heatmap vertically to pan frequency.

## IQ capture mode

The header **Source** dropdown switches to an independent library of I/Q
captures. Each one renders as a multi-resolution STFT spectrogram pyramid
(`iq.duckdb`) on its **own** axes: elapsed time within the capture on X,
`fc ± fs/2` on Y, with the same continuous zoom and locked colours. The example
below is a NIST high-SNR FDD-LTE uplink capture showing the burst / resource-block
structure.

![IQ capture spectrogram](docs/iq_capture.jpg)

# Architecture

- **`serve.py`**: the whole backend (Flask). It serves `viewer.html` and all the
  APIs (`/api/heatmap`, `/api/psd_layer`, `/api/pfp_frame`, `/api/iq_*`, plus the
  `*_meta` availability endpoints). For each request it picks the coarsest stored
  level that still has detail for the window and renders WebP tiles (numpy +
  Pillow; axis metadata rides in the `X-Meta` response header). It reads the
  DuckDB files read-only, with a per-request connection so renders are thread-safe.
- **`viewer.html`**: single-file canvas app, no build step. Zoom/pan, the layer
  swaps, the zoom sliders, and colour locking all live here.
- **`ingest/`**: the data-build tooling (see below). `sigmf_io.py` there is a
  unified reader for IQ capture formats (SigMF / TDMS / npy).

## Data storage

Quantized **int8** spectra/frames, grouped into **zlib-compressed chunks** of
consecutive captures. Summary tables store dBm as `SMALLINT` (dBm×10). The IQ
pyramid stores int8 STFT columns with a fixed per-capture `vmin/vmax` so colours
never drift on zoom. The DuckDB files are multi-GB and **not** checked into git;
build them with the scripts below (or run the demo, which builds a small one).

# Building the databases

The `ingest/` scripts read a source dataset and write the compact DuckDB files
next to `serve.py`. All are resumable.

| Script | Builds | Notes |
|--------|--------|-------|
| `ingest/build_db.py` | `spectrum.duckdb` | summaries + pyramid levels (lvl_m10/h1/h6/d1) |
| `ingest/psd_ingest.py` | `psd.duckdb` | PSD `max`, int8 per capture |
| `ingest/pfp_ingest.py` | `pfp.duckdb` | PFP `max_peak`, int8 per channel/capture |
| `ingest/master_ingest.py` | both CBRS DBs | ingests all 10 sensors, then swaps build → live |
| `ingest/compact_db.py` | `*_c.duckdb` | **run after any CBRS ingest**: repacks into the chunked/zlib schema the server reads |
| `ingest/iq_ingest.py` | `iq.duckdb` | STFT pyramid for IQ captures (SigMF/TDMS/npy) |

```bash
# point at your copy of the CBRS source data (only for ingest / on-demand drill)
export SEA_DATA_ROOT="/path/to/SEA-DATA"   # PowerShell: $env:SEA_DATA_ROOT="..."

# ingest one IQ dataset folder
python ingest/iq_ingest.py /path/to/sigmf_or_tdms_folder --dataset my_dataset
```

Per-dataset format notes for the four NIST I/Q sets are in
[docs/IQ_DATASETS.md](docs/IQ_DATASETS.md).

# Bring your own dataset

The ingest scripts all read from local disk; `ingest/fetch.py` is the step
before them. Point it at a NIST dataset and it puts the files on disk in the
layout the ingest scripts expect. Two steps, any dataset, including ones NIST
publishes after this was written:

1. **Fetch**: `python ingest/fetch.py <NIST PDR record URL or direct file URL>`
   downloads the files (streamed and resumable: Ctrl+C and re-run to continue;
   `.sha256` sidecars are verified when the record has them).
2. **Ingest**: run the matching ingest script, same as above.

A dataset's DOI landing page is a JS app, but every NIST Public Data
Repository record has a JSON manifest at `data.nist.gov/rmm/records/<id>`
listing each file's path, size and download URL. `fetch.py` takes the record
id, the landing/DOI URL, or a direct file URL; `--list` previews a record and
`--filter` narrows it (full flags: `python ingest/fetch.py --help`).

**IQ example**: a small slice of the FDD-LTE record
[mds2-3177](https://data.nist.gov/od/id/mds2-3177), rendered end to end:

```bash
python ingest/fetch.py mds2-3177 --list                    # see what's inside (~34 GB total)
python ingest/fetch.py mds2-3177 --filter 1.4MHz/config_0 --dest iqdata/mds2-3177   # one 37 MB capture
python ingest/iq_ingest.py iqdata/mds2-3177 --dataset mds2-3177
python serve.py                                            # Source dropdown -> the new capture
```

**CBRS SEA example**: the [SEA data portal](https://pages.nist.gov/SEA-DATA/)
designates PDR record [mds2-4214](https://data.nist.gov/od/id/mds2-4214)
(not yet published as of July 2026; its Box mirror is invite-only, which
`fetch.py` deliberately doesn't touch). The day the record goes live:

```bash
export SEA_DATA_ROOT="/path/to/SEA-DATA"
python ingest/fetch.py mds2-4214 --filter CBBT-Directional --dest "$SEA_DATA_ROOT"
python ingest/psd_ingest.py CBBT-Directional         # then pfp_ingest.py, compact_db.py
```

Summaries CSVs for `build_db.py` go flat into `ingest/csv/`: add
`--dest ingest/csv --flat`.

Handing this to a Claude / Cowork session? Paste:

```text
In the spectrum-viewer repo: given this NIST PDR URL <URL>, run
`python ingest/fetch.py <URL> --list` to see the record, fetch a SMALL slice
(--filter) into the layout the matching ingest script expects, then run that
script unmodified: build_db.py for Summaries CSVs fetched flat into
ingest/csv; psd_ingest.py / pfp_ingest.py with SEA_DATA_ROOT set to the
fetch --dest; iq_ingest.py <folder> --dataset <name> for SigMF/TDMS/npy.
After any CBRS ingest run compact_db.py then vacuum_dbs.py. Finish by
confirming the new source renders in serve.py.
```

# Repository layout

```
serve.py            Flask backend (app entry point)
viewer.html         single-file canvas frontend
requirements.txt
examples/           zero-download synthetic demo (make_sample.py)
docs/               screenshots + IQ dataset notes
ingest/             database-build tooling (CBRS + IQ), all resumable
```

This repo contains **only code**, no spectrum data. The measurements are public
NIST datasets; the ingest scripts expect their file products laid out locally.

# Contact

Jimmy Lu ([@Flu-Cloud](https://github.com/Flu-Cloud))

# NIST data acknowledgment & disclaimer

This project visualizes public datasets from the National Institute of Standards
and Technology (NIST): the NASCTN CBRS SEA monitoring data and the Applied Systems
Metrology Group I/Q data sets. It is an independent visualization tool and is not
endorsed by NIST.

Certain commercial equipment, instruments, software, or materials may be
identified in this repository to foster understanding. Such identification does
not imply recommendation or endorsement by the National Institute of Standards
and Technology, nor does it imply that the materials or equipment identified are
necessarily the best available for the purpose.
