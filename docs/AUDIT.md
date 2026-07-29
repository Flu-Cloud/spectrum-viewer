# ATLAS install and data-pipeline audit

> **Status.** This documents the state *before* the fetch and database-building
> fixes. Now closed: F1-F7 (fetch source parsing, host policy, network errors,
> retries, listing), P1-P5 and P7 (the spectrum.duckdb dependency, the schema
> mismatch, the silent empty database, the fixed folder layout, the unchecked
> column count), and the Windows-only parts of P6. Regression cover lives in
> `examples/test_fetch.py` and `examples/test_ingest.py`.
> Still open: F8 and D1-D3, which are the single-entry-point work
> (`atlas.py`, `environment.yml`, `datasets.json`, the prebuilt-database path)
> described in [MASTER_PROMPT.md](MASTER_PROMPT.md).

What actually happens when someone follows the README on a clean machine, tested
end to end rather than read off the page. Every item marked VERIFIED below was
reproduced on a wiped environment; items marked RISK are reasoned from the code
and could not be executed here because the test sandbox blocks `data.nist.gov`.

Test environment: Linux, Python 3.11.15 (system) and Python 3.12.13 (Miniforge
conda env), both created from scratch. `pypi.org` reachable, `data.nist.gov`
refused at the network layer (proxy answered 403 to CONNECT), so the live
download path could not be exercised. Everything else was run for real.

## 1. The install is not the problem

Both clean paths pass, quickly, with no manual steps:

| Path | Python | `pip install -r requirements.txt` | `examples/verify.py` |
|---|---|---|---|
| `python -m venv` | 3.11.15 | 9.5 s | RESULT: PASS |
| Miniforge / conda env | 3.12.13 | 9.1 s | RESULT: PASS |

`verify.py` builds the synthetic capture, ingests it, and exercises `/`,
`/api/iq_index`, `/api/iq_meta` and `/api/iq_layer` in-process. All eight checks
pass. The demo renders an 88 KB WebP tile.

So the quick start works. The failure is entirely in the "bring your own
dataset" half: getting real data from NIST and getting it into a database the
server can read.

## 2. Verified defects

### Fetch layer (`ingest/fetch.py`)

**F1. A documented input crashes. VERIFIED.**
The module docstring (line 11) advertises `ark:/88434/mds2-3177`. That input
raises `can't interpret source`. `re.match(r"^[a-z]+:", s)` at line 103 matches
the `ark:` prefix, so the bare-token branch that would have handled it is dead
code for exactly the input it was written for.

**F2. The host allowlist refuses rather than warns. VERIFIED.**
`host_ok` (lines 68-75) accepts only `https://*.nist.gov` plus one hardcoded
bucket, `nist-oar-cache.s3.amazonaws.com` (line 53). Rejected: `s3.amazonaws.com`
(which NIST also uses for MIDAS objects), Zenodo, figshare, any institutional
mirror, any plain file server. Worse, `record_files` (line 168) applies the same
test per component and drops non-matching ones with a warning; a record whose
files live on a cache host not on the list yields "no downloadable files in this
record" with no way to override. There is no `--allow-host` and no `--any-host`.

**F3. The allowlist is internally inconsistent. VERIFIED.**
`https://nist-oar-cache.s3.amazonaws.com/x.tdms` is an allowed *redirect*
target but `parse_source` rejects it as a *source*. The same URL is legal
mid-download and illegal on the command line.

**F4. `http://` gets the wrong diagnosis. VERIFIED.**
`http://data.nist.gov/rmm/records/mds2-3177` parses fine, then dies with "not
an allowed NIST host". It is a NIST host. The problem is the scheme, and the
message says otherwise.

**F5. Network failures surface as a raw traceback. VERIFIED.**
Behind a proxy the run ends in a 20-line stack trace terminating in
`OSError: Tunnel connection failed: 403 Forbidden`. Nothing catches
`URLError`/`OSError`/`ssl.SSLError`, nothing mentions `HTTPS_PROXY`, nothing
retries. This is the single most likely first experience on a corporate or
campus network.

**F6. No retry or backoff.** A transient 5xx or a dropped connection mid-record
aborts the loop. Resume works only because the user re-runs by hand.

**F7. `--list` does not scale.** On a 902-file record it prints 1804 lines
(path and URL per file) with no grouping, no tree, no summary of the folder
structure the user needs in order to choose a `--filter`.

**F8. It deliberately stops short.** The docstring states it "never runs ingest
for you". The user is then required to know which of four ingest scripts
matches what they just downloaded, and what directory layout that script wants.

### Pipeline wiring

**P1. PSD/PFP ingest hard-depends on a database the README never tells you to
build. VERIFIED. This is the error in the screenshot.**
`psd_ingest.days_for()` (line 46) opens `spectrum.duckdb` read-only purely to
enumerate which days exist. `pfp_ingest.py` does the same at line 45. On a
fresh clone:

```
_duckdb.IOException: IO Error: Cannot open database
"/home/user/ATLAS/spectrum.duckdb" in read-only mode: database does not exist
```

`spectrum.duckdb` comes from `build_db.py`, which needs Summaries CSVs in
`ingest/csv/`, which the README's CBRS recipe never fetches. Following the
README verbatim is guaranteed to produce this crash. The prerequisite is real
but invisible, and the failure is a raw DuckDB exception rather than a sentence.

**P2. The server reads a different schema than the ingest scripts write.
VERIFIED by inspection.**
`psd_ingest.py` writes table `psd` into `psd.duckdb`. `serve.py` (line 242)
queries `psd_chunk`. The chunk schema is produced by `compact_db.py`, which
writes to `psd_c.duckdb` (line 99) and never renames it. The required
`psd_c.duckdb -> psd.duckdb` swap exists only in prose inside two module
docstrings. It is absent from every command list in the README.

**P3. Skipping the swap produces a viewer that lies. VERIFIED.**
Built a `psd.duckdb` in the row-per-capture schema and drove the server
in-process:

```
GET /api/psd_meta  -> {"has": true, "fmin": 3530040000.0, "fmax": 3709960000.0, ...}
GET /api/psd_layer -> HTTP 500
   _duckdb.CatalogException: Table with name psd_chunk does not exist!
```

`psd_meta` exists in *both* schemas, so the availability probe passes and the
viewer advertises a PSD layer it cannot draw. Same structure for PFP.

**P4. Missing source data is a silent success. VERIFIED.**
With `spectrum.duckdb` present but no source CSVs on disk, `psd_ingest.py`
prints "4 days to ingest", then:

```
Done in 0.0 min. CBBT-Directional: 0 captures. DB psd.duckdb = 0.00 GB
```

Exit code 0. It writes a `psd_meta` row with `t_min = NULL, t_max = NULL,
captures = 0`. The user gets a database that looks built, an exit code that says
success, and a server that will report `has: true` with null time bounds. The
`miss` counter is incremented per absent file (line 96-97) and never checked.

**P5. Fetch output layout and ingest input layout are not the same shape. RISK.**
`psd_ingest.py` looks for `$SEA_DATA_ROOT/PSD/<day>_<sensor>_max.csv` (line 94);
`pfp_ingest.py` looks for `$SEA_DATA_ROOT/PFP Aligned/PFP_<day>_<sensor>_<stat>.csv`
(line 96). Those folder names ("PFP Aligned", with a space) read like a Box share,
not a PDR record. `fetch.py` preserves the *record's* own `filepath` structure
under `--dest`. Unless the published record happens to use those exact directory
names, `--dest "$SEA_DATA_ROOT"` lands the files where the ingest script will not
look, and the result is P4: a clean exit and an empty database. Could not be
confirmed because mds2-4214 is not published and the sandbox cannot reach PDR.

**P6. `master_ingest.py` is Windows-only and swaps in the broken schema.
VERIFIED by inspection.**
`kill_server()` shells out to `netstat -ano` and `taskkill` (lines 54-60), so it
is inert on macOS and Linux. Its swap (lines 83-89) moves `psd_build.duckdb`
onto `psd.duckdb`, which is the row-per-capture schema, which is precisely the
P3 failure. Its own docstring (lines 12-15) admits this and asks the user to
remember to do something else instead.

**P7. Quantization constants are hardcoded to one band.**
`F0 = 3530040000.0`, `DF = 80000.0`, `NF = 2250`, `QMIN/QMAX = -180/-90` in
`psd_ingest.py` (lines 34-37). Nothing checks that the CSV actually has 2250
columns. A different sensor configuration is quantized against the wrong range
without complaint.

### Discoverability

**D1. No way to ask "what state am I in".** Five databases can exist
(`spectrum`, `psd`, `pfp`, `iq`, plus the `_c` build files), each in one of two
schemas, and there is no command that reports which are present, which schema
they hold, how many rows, and what the next step is.

**D2. Both README dataset recipes are unrunnable as printed.** The CBRS one is
proven broken at its third line (P1). The IQ one could not be executed here.

**D3. No conda entry point.** The user explicitly wants Miniforge. There is no
`environment.yml`, so the conda path is "make an env yourself, then use pip",
which works but is not written down anywhere.

## 3. Quantified: 16 defects, 6 changes

The defects collapse into six pieces of work. Estimated 10 files touched.

| # | Change | Fixes | Rough size |
|---|---|---|---|
| 1 | `atlas.py`: one CLI (`setup`, `demo`, `get`, `status`, `serve`) that detects payload type, routes to the right ingest, compacts, swaps, and serves | F8, D1, D2 | ~350 new lines |
| 2 | Rewrite the fetch host policy: any `https://` allowed, warn instead of refuse when off `nist.gov`, `--allow-host`/`--any-host`, fix `ark:`, fix the `http://` message, allow the OAR cache as a direct source, catch network/proxy/TLS errors into one actionable line, add retry with backoff, group `--list` output by folder | F1-F7 | ~150 changed lines |
| 3 | Enumerate days from files on disk (recursive glob, layout-agnostic), use `spectrum.duckdb` only as an optional refinement, never raise a bare DuckDB exception | P1, P5 | ~60 changed lines |
| 4 | Make the schema swap impossible to get wrong: `serve.py` reads either schema, every `*_meta` endpoint probes the table it will actually query before answering `has: true`, and `compact_db.py` performs the swap itself with a `.bak` | P2, P3, P6 | ~80 changed lines |
| 5 | Fail loudly on zero input: non-zero exit, print what was searched for and what was actually found nearby, refuse to write a meta row for an empty ingest, validate column count against `NF` | P4, P7 | ~50 changed lines |
| 6 | `environment.yml` + a bootstrap script, rewritten README with copy-pasteable commands, `datasets.json` registry of friendly names, and a CI job that runs the whole documented path | D2, D3 | ~200 lines + docs |

## 4. What "easiest on any setup" should look like

Today the CBRS path is seven commands plus one undocumented rename, and two of
those commands cannot succeed in the printed order. The target is two commands:

```
python atlas.py setup          # makes the env, installs, builds the demo, self-checks
python atlas.py get cbrs       # fetches, detects, ingests, compacts, swaps, serves
```

`get` should take any of: a friendly name from `datasets.json`, a PDR record id,
a DOI, a landing URL, a direct file URL, or **a local folder that is already on
disk**. That last one matters: the most common real situation is a user who
already downloaded the data by other means and just wants it ingested. Right now
that user has to reverse-engineer the expected directory layout.

The strategic fix the project should aim for, which also answers "publish the
DBs on PDR and point at PDR": publish the **built** `.duckdb` files as their own
PDR record, and give `atlas.py get --prebuilt <id>` a path that downloads a
finished database straight next to `serve.py`. That removes ingest, compaction,
schema swaps, source-CSV layout, `SEA_DATA_ROOT`, and roughly 200 GB of
downloads from the layman's critical path entirely. It turns the whole of
section 2's pipeline wiring into an optional developer workflow instead of the
front door.

## 5. Reproduction commands

```bash
# install, clean venv
python -m venv /tmp/cleanvenv && /tmp/cleanvenv/bin/pip install -r requirements.txt
/tmp/cleanvenv/bin/python examples/verify.py          # RESULT: PASS

# F1, F3, F4: URL handling
python -c "import sys; sys.path.insert(0,'ingest'); import fetch; print(fetch.parse_source('ark:/88434/mds2-3177'))"

# P1: the screenshot error
python ingest/psd_ingest.py CBBT-Directional          # IOException, spectrum.duckdb missing

# P4: silent empty database (needs a stub spectrum.duckdb with a `raw` table)
python ingest/psd_ingest.py CBBT-Directional          # exit 0, 0 captures, NULL time bounds
```
