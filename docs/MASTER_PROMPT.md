# Master prompt

Paste the block below to a coding agent working in a clone of this repo. It is
self-contained: every defect it names was reproduced first, so the agent does
not need to rediscover them. Evidence and line references are in
[AUDIT.md](AUDIT.md).

> **Already done on this branch:** defects F1-F7 and P1-P7, and build items 2,
> 3, 4 and 5 below. `examples/test_fetch.py` and `examples/test_ingest.py`
> cover them. What remains is build items 1, 6, 7 and 8: the single `atlas.py`
> entry point, the state report, the conda bootstrap and dataset registry, and
> the prebuilt-database download path. Acceptance criteria A2, A3, A4, A6 and
> A7 already pass; A1, A5 and A8 depend on the remaining items.

---

```text
You are working in the ATLAS repo (https://github.com/jimmylu7/ATLAS), a Flask +
DuckDB spectrum viewer. The quick-start demo already installs and passes on a
clean machine in about 10 seconds; do not rewrite it. The broken half is
everything after "Bring your own dataset": getting real NIST data onto disk and
into a database the server can actually read.

GOAL
Reduce the real-data path from seven commands plus one undocumented file rename
to two commands that work on Windows, macOS and Linux, on a bare venv or a
Miniforge/conda env, for a user who knows nothing about the internals. Do not
add any new runtime dependency; fetching must stay on the standard library.

CONFIRMED DEFECTS. Each was reproduced. Fix all of them.

Fetch layer, ingest/fetch.py:
 F1. The docstring advertises "ark:/88434/mds2-3177" but that input raises
     "can't interpret source". The regex ^[a-z]+: at line 103 matches the "ark:"
     prefix, so the branch written to handle it is unreachable.
 F2. host_ok() accepts only https://*.nist.gov plus the single hardcoded bucket
     nist-oar-cache.s3.amazonaws.com, and record_files() applies the same test
     per component, silently dropping the rest. A record served from any other
     host yields "no downloadable files in this record" with no override.
 F3. nist-oar-cache.s3.amazonaws.com is a legal redirect target but is rejected
     by parse_source() as a source. Same URL, two answers.
 F4. An http:// NIST URL parses, then fails with "not an allowed NIST host".
     It is a NIST host; the scheme is the problem and the message misleads.
 F5. Any network failure is an uncaught traceback. Behind a proxy the run ends
     in OSError: Tunnel connection failed: 403 Forbidden with a stack trace.
     Nothing catches URLError/OSError/ssl.SSLError, nothing mentions HTTPS_PROXY.
 F6. No retry or backoff on transient failures.
 F7. --list prints two lines per file. On a 902-file record that is 1804 lines
     with no folder grouping, which is useless for choosing a --filter.
 F8. fetch.py refuses on principle to run ingest, so the user must know which of
     four ingest scripts matches the bytes they just downloaded, and what
     directory layout that script expects.

Pipeline wiring:
 P1. psd_ingest.days_for() and pfp_ingest.days_for() open spectrum.duckdb
     read-only purely to enumerate days, and die with a raw
     _duckdb.IOException when it is absent. spectrum.duckdb comes from
     build_db.py, which needs Summaries CSVs in ingest/csv/, which the README's
     CBRS recipe never fetches. Following the README verbatim is guaranteed to
     hit this. This is the reported crash.
 P2. psd_ingest.py writes table `psd` into psd.duckdb. serve.py queries
     `psd_chunk`. The chunk schema comes from compact_db.py, which writes
     psd_c.duckdb and never renames it. The required psd_c.duckdb ->
     psd.duckdb swap appears only in prose inside two module docstrings and in
     no command list. Same for pfp.
 P3. Because psd_meta exists in BOTH schemas, skipping the swap makes
     /api/psd_meta answer {"has": true} while /api/psd_layer returns HTTP 500
     with "Catalog Error: Table with name psd_chunk does not exist". The viewer
     advertises a layer it cannot draw. Same for pfp.
 P4. When the source CSVs are not where the script looks, psd_ingest.py exits 0,
     prints "Done ... 0 captures", and writes a psd_meta row with t_min NULL,
     t_max NULL, captures 0. A silent empty database that reports success. The
     `miss` counter is incremented and never checked.
 P5. psd_ingest.py looks for $SEA_DATA_ROOT/PSD/<day>_<sensor>_max.csv and
     pfp_ingest.py for "$SEA_DATA_ROOT/PFP Aligned/PFP_<day>_<sensor>_<stat>.csv".
     Those are Box-share folder names. fetch.py preserves the PDR record's own
     structure under --dest, which will not generally match, and the mismatch
     lands as P4.
 P6. master_ingest.py calls netstat and taskkill, so it is inert off Windows,
     and its build->live swap installs the row-per-capture schema, which is
     exactly the P3 failure. Its own docstring admits this.
 P7. F0/DF/NF and QMIN/QMAX in psd_ingest.py are hardcoded to the CBRS band and
     nothing validates that a CSV actually has NF columns.

WHAT TO BUILD

1. atlas.py at the repo root: the single entry point. Subcommands:
     setup      create or detect the environment, install requirements, build
                the demo, run examples/verify.py, print one verdict line
     demo       build the synthetic capture and serve it
     status     the state report described in item 6
     get TARGET fetch (if remote), detect, ingest, compact, swap, verify
     serve      start the server
   TARGET accepts: a friendly name from datasets.json, a PDR record id, an
   ark: id, a DOI, a landing URL, a direct file URL, OR A LOCAL PATH THAT IS
   ALREADY ON DISK. The local-path case is the most common real situation and
   must be first-class, not an afterthought. Add --dry-run to every subcommand
   that writes, printing the plan and touching nothing.
   Keep the existing scripts working when invoked directly; atlas.py calls into
   them rather than duplicating them.

2. Rewrite the fetch host policy. Allow any https:// URL by default. When the
   host is not under nist.gov, print one warning line and continue; do not
   refuse. Add --allow-host HOST (repeatable) and --any-host. Keep refusing
   plaintext http:// by default but say so accurately ("http:// is not
   encrypted; pass --allow-http to override"), and add that flag. Accept
   nist-oar-cache.s3.amazonaws.com and any host reached by redirect from an
   allowed host. Fix the ark: parse. Wrap every network call so URLError,
   OSError, socket.timeout and ssl.SSLError become one actionable sentence that
   names the host, the cause, and HTTPS_PROXY when a proxy is configured. Add
   three retries with exponential backoff on transient errors and on partial
   reads. Make --list group by directory with per-folder counts and totals, and
   add --tree for structure only.

3. Break the spectrum.duckdb dependency in psd_ingest.py and pfp_ingest.py.
   Enumerate work units by recursively globbing the data root for files whose
   names match the expected pattern, regardless of intermediate folder names, so
   the fetch layout and the Box layout both work. Use spectrum.duckdb only when
   it exists, and only to narrow the day list. Never let a bare DuckDB exception
   reach the user.

4. Make the schema swap impossible to get wrong. Do all three:
   a. serve.py reads either schema: query psd_chunk when that table exists,
      otherwise read psd directly, and likewise for pfp. Detect once per
      connection.
   b. Every *_meta endpoint probes the table it will actually query before
      answering has: true. No endpoint may advertise a layer whose backing table
      is missing.
   c. compact_db.py performs the swap itself at the end, moving the original to
      <name>.duckdb.bak first, with --no-swap to opt out.

5. Fail loudly on an empty ingest. If zero input files matched, exit non-zero,
   print the exact glob that was searched, and list what does exist in the
   nearest populated directory so the user can see the shape mismatch. Never
   write a meta row for an ingest that produced zero rows. Validate that each
   CSV's column count matches NF and name the file when it does not.

6. atlas.py status prints, for each of spectrum/psd/pfp/iq: present or absent,
   which schema, row counts, time range, file size, and one recommended next
   command. It must be safe to run at any point and must never modify anything.

7. environment.yml for Miniforge, plus bootstrap scripts (bootstrap.sh and
   bootstrap.ps1) that create the environment and run atlas.py setup. Add
   datasets.json mapping friendly names to PDR ids and to the ingest each needs.
   Rewrite the README's data sections so every printed command is one that CI
   actually runs. Delete or clearly quarantine master_ingest.py and run_master.ps1
   rather than leaving Windows-only code that installs a broken schema.

8. Add a prebuilt path: atlas.py get --prebuilt <pdr-id> downloads finished
   .duckdb files from a PDR record straight next to serve.py, with checksum
   verification, and does no ingest at all. Wire datasets.json so a name can
   point at either a prebuilt record or a raw-source record. This is the path a
   layman should take once the built databases are published on NIST PDR, and it
   should be the documented default the moment such a record exists.

ACCEPTANCE CRITERIA. Do not report success until each of these has actually been
run and its real output pasted into your summary.

 A1. From a wiped clone, on a bare venv AND on a fresh conda env:
     python atlas.py setup && python atlas.py status
     ends with a PASS verdict, and status reports iq present, CBRS absent.
 A2. python ingest/psd_ingest.py CBBT-Directional with no spectrum.duckdb and no
     source data prints one actionable sentence and exits non-zero. No traceback.
 A3. Build a psd.duckdb in the row-per-capture schema, start the server, and show
     /api/psd_meta and /api/psd_layer agreeing: either both work, or has is false.
     No HTTP 500 anywhere.
 A4. Point an ingest at a directory with no matching files: non-zero exit, the
     searched pattern printed, no psd_meta row written.
 A5. Stage a directory of PSD-shaped CSVs under an arbitrary nested path, run
     atlas.py get <that path>, and show the capture rendering through
     /api/psd_layer with no manual rename anywhere in the transcript.
 A6. fetch.py handles all of these without a traceback: mds2-3177,
     ark:/88434/mds2-3177, a DOI URL, a landing URL, a direct file URL, an
     http:// URL, a non-NIST https URL, and an unreachable host.
 A7. examples/verify.py still ends in RESULT: PASS, and the synthetic demo still
     renders.
 A8. Grep the final README for every fenced command and confirm each one is
     exercised by CI or by a test.

CONSTRAINTS
 - No new runtime dependencies. fetch.py stays standard library only.
 - Cross-platform: no netstat, no taskkill, no shell-specific quoting in any
   documented command. Give the PowerShell form wherever an env var is set.
 - Match the existing code style. No em dashes anywhere in code, comments, or
   docs; this repo has explicitly removed them.
 - Do not modify viewer.html except where an endpoint contract genuinely changes.
 - If you cannot reach data.nist.gov from your environment, say so plainly and
   test the fetch layer against a local HTTP fixture instead. Do not claim a
   download path works when you have not run it.
```
