# NIST-Lu: Spectrum Viewer

"Google Maps for RF spectrum": one interface where **X is always time**
and color is power. Zoom from a TWO-YEAR overview down to the microsecond
structure instantly & in a single page. The viewer swaps resolution layers automatically as
you go.

![CBRS spectrum overview](docs/cbrs_overview.jpg)

It draws two independent data sources on the same machinery:

- **CBRS SEA monitoring**: the [NIST NASCTN CBRS SEA](https://www.nist.gov/programs-projects/spectrum-monitoring-cbrs-band)
  dataset (10 sensors, 3.5 GHz band, ~2 years), as one continuous time axis.
- **IQ captures**: STFT spectrograms of individual [NIST I/Q recordings](https://www.nist.gov/ctl/spectrum-technology-and-research-division/applied-systems-metrology-group/iq-data-sets)
  (SigMF / TDMS), each with its own time and frequency axes.

The real datasets are multi-GB and are **not** in this repo. A built-in synthetic
demo lets you run everything with no downloads.

# Quick start

**You need:** Python 3.10 or newer, `git`, and a web browser. Check Python with
`python --version` (if that says "not found", try `python3 --version`, or `py
--version` on Windows).

Below, use whichever of `python` / `python3` / `py` works on your machine.

**1. Get the code**

```bash
git clone https://github.com/Flu-Cloud/spectrum-viewer.git
cd spectrum-viewer
```

**2. Create a virtual environment, then install**

A virtual environment is required on most Linux and macOS systems (a bare `pip
install` there fails with "externally-managed-environment").

```bash
python -m venv .venv                  # creates a local .venv folder

# activate it:
source .venv/bin/activate             # macOS / Linux
.venv\Scripts\activate                # Windows PowerShell

pip install -r requirements.txt
```

**3. Build the demo data and check the install**

```bash
python examples/verify.py
```

This builds a small synthetic capture (`iq.duckdb`, ~2 MB, created next to
`serve.py`) and tests every part of the app. It must end with:

```
RESULT: PASS
```

If it says `FAIL`, the line above it names the problem. See
[Troubleshooting](#troubleshooting).

**4. Start the viewer**

```bash
python serve.py
```

Open **http://127.0.0.1:8090** in your browser. In the header, open the
**Source** dropdown and pick **"IQ capture: demo_signal"**.

![Synthetic demo spectrogram](docs/demo_spectrogram.png)

You should see a horizontal line (a carrier), stepping horizontal
segments (a hopping tone), and evenly spaced vertical bands (periodic bursts).
Scroll to zoom in time, `Ctrl`+scroll to zoom in frequency, drag to pan. To
stop the server, press `Ctrl+C` in the terminal.

That is the whole install. Everything below is optional.

# Troubleshooting

| Symptom | Fix |
|---|---|
| `python: command not found` | Use `python3` (macOS/Linux) or `py` (Windows). |
| `error: externally-managed-environment` | You skipped the virtual environment in step 2. |
| `ModuleNotFoundError: No module named 'duckdb'` | The venv isn't active, or step 2's `pip install` didn't run. Re-activate and re-run it. |
| `Address already in use` / page won't load | Port 8090 is taken. Run on another port: `SEA_PORT=8095 python serve.py` (PowerShell: `$env:SEA_PORT=8095; python serve.py`), then open that port. |
| Browser shows "No spectrum data found" | Run `python examples/verify.py` — the demo database wasn't built. |
| Server prints "spectrum.duckdb not found" | Expected. That's the optional multi-GB CBRS data; IQ mode still works. |

# For AI agents (Claude Code, Cowork, etc.)

Paste this to have an agent entirely install the project:

```text
Clone https://github.com/Flu-Cloud/spectrum-viewer.git and install it.
Create a virtual environment, install requirements.txt into it, then run
`python examples/verify.py` and show me the output. Do not modify any
repository files. The install is correct only if that command prints
"RESULT: PASS"; if it prints FAIL, report the failing check verbatim and stop.
Finally, start `python serve.py` and confirm http://127.0.0.1:8090 responds.
```

`examples/verify.py` is the main tell for "did this work": it
checks the Python version, the imports, the demo database and every API
endpoint in-process (no browser or free port required), and exits 0 on success.

# What the two modes show

## CBRS monitoring mode

One continuous interface where X is always time. As you zoom in, the viewer swaps
between three stored-resolution layers automatically:

| Layer | Source | Resolution | Shown when |
|-------|--------|------------|------------|
| **Summary** | `spectrum.duckdb` | ~90 s sweeps, 18 channels | zoomed out (months → days) |
| **PSD** | `psd.duckdb` | 2250 freq bins @ 80 kHz, ~4-min captures | time span ≤ 3 days |
| **PFP** | `pfp.duckdb` | 560 pts across a 10 ms frame (~18 µs/pt) | freq narrowed to ~1 channel, or span ≤ 30 min |

Frequency zoom is the vertical slider on the left (drag up = zoom in); drag the
screen vertically to pan frequency.

## IQ capture mode

The **Source** dropdown switches to an independent library of I/Q captures. Each
renders as a multi-resolution STFT spectrogram pyramid (`iq.duckdb`) on its own
axes: elapsed time within the capture on X, `fc ± fs/2` on Y, with the same
continuous zoom and locked colours. Below is a NIST high-SNR FDD-LTE uplink
capture showing the burst / resource-block structure.

![IQ capture spectrogram](docs/iq_capture.jpg)

# Bring your own dataset

`ingest/fetch.py` downloads a NIST dataset into the layout the ingest scripts
expect; then the matching ingest script builds the database.

A dataset's DOI landing page is a JavaScript app, but every NIST Public Data
Repository record has a JSON manifest at `data.nist.gov/rmm/records/<id>`
listing each file's path, size and download URL. `fetch.py` accepts the record
id, the landing/DOI URL, or a direct file URL. `--list` previews a record and
`--filter` narrows it (all flags: `python ingest/fetch.py --help`). Downloads
are resumable — press `Ctrl+C` and re-run to continue.

**IQ example**, a small slice of FDD-LTE record
[mds2-3177](https://data.nist.gov/od/id/mds2-3177), end to end:

```bash
python ingest/fetch.py mds2-3177 --list                    # 902 files, ~189 GB total
python ingest/fetch.py mds2-3177 --filter 1.4MHz/config_0 --dest iqdata/mds2-3177   # one 37 MB capture
python ingest/iq_ingest.py iqdata/mds2-3177 --dataset mds2-3177
python serve.py                                            # Source dropdown -> the new capture
```

**CBRS SEA example**: the [SEA data portal](https://pages.nist.gov/SEA-DATA/)
designates PDR record [mds2-4214](https://data.nist.gov/od/id/mds2-4214) (not
yet published as of July 2026; its Box mirror is invite-only, which `fetch.py`
deliberately doesn't touch). The day the record goes live:

```bash
export SEA_DATA_ROOT="/path/to/SEA-DATA"    # PowerShell: $env:SEA_DATA_ROOT="..."
python ingest/fetch.py mds2-4214 --filter CBBT-Directional --dest "$SEA_DATA_ROOT"
python ingest/psd_ingest.py CBBT-Directional         # then pfp_ingest.py, compact_db.py
```

Summaries CSVs for `build_db.py` go flat into `ingest/csv/`: add
`--dest ingest/csv --flat`.

Want to utilize AI (Claude Cowork / Code session)? Paste:

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

Per-dataset format notes for the four NIST I/Q sets are in
[docs/IQ_DATASETS.md](docs/IQ_DATASETS.md).

# Building the databases

The `ingest/` scripts write the DuckDB files next to `serve.py`. All are
resumable. Set `SEA_DATA_ROOT` to your copy of the CBRS source CSVs (it
defaults to `./SEA-DATA` inside the repo).

| Script | Builds | Notes |
|--------|--------|-------|
| `ingest/fetch.py` | (downloads) | pulls a NIST record onto disk |
| `ingest/build_db.py` | `spectrum.duckdb` | summaries + pyramid levels (lvl_m10/h1/h6/d1) |
| `ingest/psd_ingest.py` | `psd.duckdb` | PSD `max`, int8 per capture |
| `ingest/pfp_ingest.py` | `pfp.duckdb` | PFP `max_peak`, int8 per channel/capture |
| `ingest/master_ingest.py` | both CBRS DBs | all 10 sensors, then swaps build → live |
| `ingest/compact_db.py` | `*_c.duckdb` | **run after any CBRS ingest**: repacks into the chunked/zlib schema the server reads |
| `ingest/iq_ingest.py` | `iq.duckdb` | STFT pyramid for IQ captures (SigMF/TDMS/npy) |

# Architecture

- **`serve.py`**: the whole backend (Flask). Serves `viewer.html` and the APIs
  (`/api/heatmap`, `/api/psd_layer`, `/api/pfp_frame`, `/api/iq_*`, plus `*_meta`
  availability endpoints). For each request it picks the coarsest stored level
  that still has detail for the window and renders WebP tiles (numpy + Pillow;
  axis metadata rides in the `X-Meta` response header). Reads the DuckDB files
  read-only, with a per-request connection so renders are thread-safe. The CBRS
  databases are optional — without them the server still serves IQ captures.
- **`viewer.html`**: single-file canvas app, no build step. Zoom/pan, the layer
  swaps, the zoom sliders and colour locking all live here.
- **`ingest/`**: the data-build tooling. `sigmf_io.py` is a unified reader for
  IQ capture formats (SigMF / TDMS / npy).

**Data storage.** Quantized **int8** spectra/frames, grouped into
**zlib-compressed chunks** of consecutive captures. Summary tables store dBm as
`SMALLINT` (dBm×10). The IQ pyramid stores int8 STFT columns with a fixed
per-capture `vmin/vmax` so colours never drift on zoom.

```
serve.py            Flask backend (app entry point)
viewer.html         single-file canvas frontend
requirements.txt    dependencies, with tested versions
examples/           make_sample.py (demo data) + verify.py (install check)
docs/               screenshots + IQ dataset notes
ingest/             fetch + database-build tooling, all resumable
```

This repo contains **only code**, no spectrum data. The measurements are public
NIST datasets; the ingest scripts expect their file products laid out locally.

# Contact

Jimmy Lu ([@jimmylu7](https://github.com/jimmylu7)(jflu@unc.edu)(jimmy.lu@nist.gov)

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
