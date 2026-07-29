# ATLAS - Automatic Tiled Layering for Analyzing Spectra

"Google Maps for RF spectrum": one interface where **X is always time**
and color is power. Continuously zoom from a two-year overview down to the microsecond
structure, instantly. The viewer swaps resolution layers automatically as
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
git clone https://github.com/jimmylu7/ATLAS.git
cd ATLAS
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
python atlas.py setup
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
python atlas.py serve
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
| Browser shows "No spectrum data found" | Run `python atlas.py status` to see what is built, then `python atlas.py setup`. |
| Server prints "spectrum.duckdb not found" | Expected. That's the optional multi-GB CBRS data; IQ mode still works. |

# For AI agents (Claude Code, Cowork, etc.)

Paste this to have an agent entirely install the project:

```text
Clone https://github.com/jimmylu7/ATLAS.git and install it.
Create a virtual environment, install requirements.txt into it, then run
`python atlas.py setup` and show me the output. Do not modify any
repository files. The install is correct only if that command prints
"RESULT: PASS"; if it prints FAIL, report the failing check verbatim and stop.
Finally, start `python atlas.py serve` and confirm http://127.0.0.1:8090
responds.
```

`atlas.py setup` runs `examples/verify.py`, which is the main tell for "did
this work": it checks the Python version, the imports, the demo database and
every API endpoint in-process (no browser or free port required), and exits 0
on success. `python atlas.py status` answers "what is built" at any point.

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

One command covers every case. Point it at what you have:

```bash
python atlas.py get ~/Downloads/mds2-3177   # data already on your disk
python atlas.py get lte-uplink              # a name from datasets.json
python atlas.py get mds2-3177               # a NIST PDR record id
python atlas.py get https://doi.org/10.18434/mds2-3177    # a DOI or URL
python atlas.py status                      # what is built, what is next
```

It downloads only when the target is not already local, classifies every file
it finds (IQ captures, CBRS PSD or PFP exports, Summaries CSVs, or prebuilt
`.duckdb` databases), runs the ingest each one needs, and prints the resulting
state. Add `--dry-run` to see the plan without changing anything, or
`--compact` to shrink the databases afterwards. A record that publishes
finished `.duckdb` files is copied straight into place with no ingest at all.

The rest of this section is what `atlas.py get` runs underneath, for when you
want to drive it yourself.

`ingest/fetch.py` downloads a dataset; then the matching ingest script builds
the database. Each ingest script finds its own source files by name, searched
recursively, so the folder layout the download produced does not have to match
anything.

A dataset's DOI landing page is a JavaScript app, but every NIST Public Data
Repository record has a JSON manifest at `data.nist.gov/rmm/records/<id>`
listing each file's path, size and download URL. `fetch.py` accepts the record
id, an `ark:` id, the landing/DOI URL, or a direct file URL to any https host.
`--list` summarises a record by folder, `--tree` shows its structure, `--long`
prints every file, and `--filter` narrows what gets downloaded (all flags:
`python ingest/fetch.py --help`). Downloads are resumable and retry transient
network errors: press `Ctrl+C` and re-run to continue.

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
yet published as of July 2026). The day the record goes live:

```bash
python ingest/fetch.py mds2-4214 --filter CBBT-Directional --dest SEA-DATA
python ingest/psd_ingest.py --root SEA-DATA --list     # what landed on disk
python ingest/psd_ingest.py CBBT-Directional --root SEA-DATA
python ingest/pfp_ingest.py CBBT-Directional --root SEA-DATA
python serve.py
```

`--root` defaults to `$SEA_DATA_ROOT`, else `./SEA-DATA`. Nothing else has to
be built first: the PSD and PFP layers read their CSVs directly.

Summaries CSVs for `build_db.py` go flat into `ingest/csv/`: add
`--dest ingest/csv --flat`.

If you already have the data on disk, skip `fetch.py` entirely and point
`--root` at wherever it is.

**Downloading from somewhere that is not NIST** works by default; you get one
warning line naming the host. Use `--nist-only` to refuse anything off
`nist.gov`, `--allow-host HOST` to re-permit one host under it, and
`--allow-http` for an unencrypted server.

Want to utilize AI (Claude Cowork / Code session)? Paste:

```text
In the ATLAS repo: given this NIST PDR URL <URL>, run
`python ingest/fetch.py <URL> --list` to see the record, fetch a SMALL slice
(--filter) into the layout the matching ingest script expects, then run that
script unmodified: build_db.py for Summaries CSVs fetched flat into
ingest/csv; psd_ingest.py / pfp_ingest.py with SEA_DATA_ROOT set to the
fetch --dest; iq_ingest.py <folder> --dataset <name> for SigMF/TDMS/npy.
compact_db.py afterwards is optional; it only shrinks the files. Finish by
confirming the new source renders in serve.py.
```

Per-dataset format notes for the four NIST I/Q sets are in
[docs/IQ_DATASETS.md](docs/IQ_DATASETS.md).

# Building the databases

The `ingest/` scripts write the DuckDB files next to `serve.py`. All are
resumable. Pass `--root` (or set `SEA_DATA_ROOT`) to point the CBRS scripts at
your copy of the source CSVs; it defaults to `./SEA-DATA` inside the repo.

| Script | Builds | Notes |
|--------|--------|-------|
| `ingest/fetch.py` | (downloads) | pulls a record onto disk, from NIST or anywhere else |
| `ingest/build_db.py` | `spectrum.duckdb` | summaries + pyramid levels (lvl_m10/h1/h6/d1) |
| `ingest/psd_ingest.py` | `psd.duckdb` | PSD `max`, int8 per capture. `--list` shows what is on disk |
| `ingest/pfp_ingest.py` | `pfp.duckdb` | PFP `max_peak`, int8 per channel/capture |
| `ingest/master_ingest.py` | both CBRS DBs | all sensors, then swaps build → live |
| `ingest/compact_db.py` | smaller DBs | **optional.** Repacks into the chunked/zlib schema and swaps it in, keeping a `.bak` |
| `ingest/iq_ingest.py` | `iq.duckdb` | STFT pyramid for IQ captures (SigMF/TDMS/npy) |

The server reads both the schema the ingest scripts write and the compacted
one, so `compact_db.py` only changes file size and speed. Run it when the
databases get big; skip it otherwise.

## Checking a change

Three self-contained checks, none of which need a network or a browser:

```bash
python examples/verify.py        # install + the synthetic IQ demo
python examples/test_fetch.py    # fetch.py against a local fake repository
python examples/test_ingest.py   # the CBRS path, uncompacted and compacted
python examples/test_atlas.py    # atlas.py routing a mixed folder
```

# Architecture

- **`serve.py`**: the whole backend (Flask). Serves `viewer.html` and the APIs
  (`/api/heatmap`, `/api/psd_layer`, `/api/pfp_frame`, `/api/iq_*`, plus `*_meta`
  availability endpoints). For each request it picks the coarsest stored level
  that still has detail for the window and renders WebP tiles (numpy + Pillow;
  axis metadata rides in the `X-Meta` response header). Reads the DuckDB files
  read-only, with a per-request connection so renders are thread-safe. The CBRS
  databases are optional: without them the server still serves IQ captures, and
  it reads each one in whichever of the two schemas is on disk.
- **`viewer.html`**: single-file canvas app, no build step. Zoom/pan, the layer
  swaps, the zoom sliders and colour locking all live here.
- **`ingest/`**: the data-build tooling. `sigmf_io.py` is a unified reader for
  IQ capture formats (SigMF / TDMS / npy).

**Data storage.** Quantized **int8** spectra/frames, grouped into
**zlib-compressed chunks** of consecutive captures. Summary tables store dBm as
`SMALLINT` (dBm×10). The IQ pyramid stores int8 STFT columns with a fixed
per-capture `vmin/vmax` so colours never drift on zoom.

```
atlas.py            one command: setup / get / status / serve
serve.py            Flask backend (app entry point)
viewer.html         single-file canvas frontend
datasets.json       friendly names -> PDR records
requirements.txt    dependencies, with tested versions
examples/           demo data, the install check, and the offline tests
docs/               screenshots + IQ dataset notes
ingest/             fetch + database-build tooling, all resumable
```

This repo contains **only code**, no spectrum data. The measurements are public
NIST datasets; the ingest scripts expect their file products laid out locally.

# Contact

Jimmy Lu
GitHub: [@jimmylu7](https://github.com/jimmylu7)
Email: [jflu@unc.edu](mailto:jflu@unc.edu) or [jimmy.lu@nist.gov](mailto:jimmy.lu@nist.gov)

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
