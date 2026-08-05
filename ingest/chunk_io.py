"""chunk_io.py: append to a database in whichever schema is already on disk.

The PSD and PFP databases exist in two shapes. The ingests write one row per
capture (`psd`, `pfp`); `compact_db.py` rewrites that into zlib-compressed
chunks of consecutive captures (`psd_chunk`, `pfp_chunk`) and serve.py reads
either one.

The catch is what happens *after* compaction. The CBRS datasets grow, so the
normal thing is to run the ingest again next month against a database that has
already been compacted. Writing rows into it then produces a file holding both
schemas at once, and serve.py resolves that by preferring the chunk table -- so
the freshly ingested month is stored, reported as ingested, and never drawn.
The old days are worse: the resume check reads the rows table, finds it empty,
and re-reads every day the chunk table already had, duplicating the lot.

So an append has to speak whichever schema it finds:

    kind = schema_of(con, "psd")            # 'chunk' | 'rows' | 'empty'
    have = existing_days(con, "psd", sensor, kind)
    with ChunkAppender(con, "psd", sensor) as app:   # kind == 'chunk'
        app.add(times, quantized)

`existing_days` returns UTC date strings from whichever table holds the data,
which is what makes the resume comparison mean the same thing in both shapes.
"""

import zlib

import numpy as np

Z = 9                # zlib level, matching compact_db.py -- max level costs
                     # nothing at read time (decompress speed is level-independent)
                     # and measured ~2.6% smaller than level 6 on real PFP chunks
PSD_CHUNK = 256      # spectra per chunk
PFP_CHUNK = 1024     # frames per chunk

# (chunk table, row table, rows per chunk, extra key columns)
LAYOUT = {
    "psd": ("psd_chunk", "psd", PSD_CHUNK, ()),
    "pfp": ("pfp_chunk", "pfp", PFP_CHUNK, ("freq",)),
}


def schema_of(con, base):
    """Which shape this database is in: 'chunk', 'rows', or 'empty'.

    Mirrors serve.py's own detection, including its preference for the chunk
    table when a file somehow holds both -- an ingest must write where the
    server will read, not where it would rather write.
    """
    chunk, rows, _n, _k = LAYOUT[base]
    have = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables").fetchall()}
    if chunk in have and con.execute(
            f"SELECT count(*) FROM {chunk}").fetchone()[0]:
        return "chunk"
    if rows in have and con.execute(
            f"SELECT count(*) FROM {rows}").fetchone()[0]:
        return "rows"
    return "empty"


def existing_days(con, base, sensor, kind):
    """UTC dates ('YYYY-MM-DD') this sensor already has stored, either shape.

    Bucketed in UTC on both sides. `to_timestamp()` renders in the machine's
    local zone, so west of Greenwich an 00:30 UTC capture would bucket to the
    previous date and its day would be re-read on every run.
    """
    chunk, rows, _n, _k = LAYOUT[base]
    if kind == "rows":
        return {str(r[0]) for r in con.execute(
            "SELECT DISTINCT CAST(to_timestamp(t) AT TIME ZONE 'UTC' AS DATE) "
            f"FROM {rows} WHERE sensor=?", [sensor]).fetchall()}
    if kind != "chunk":
        return set()
    # A chunk stores its capture instants as a compressed float64 blob. Its
    # t0..t1 span cannot be used instead: a chunk that begins late on one day
    # and ends early on the next would mark both days complete when neither is,
    # and the missing captures would never be picked up. So read the instants.
    days = set()
    for (blob,) in con.execute(
            f"SELECT times FROM {chunk} WHERE sensor=?", [sensor]).fetchall():
        ts = np.frombuffer(zlib.decompress(blob), dtype=np.float64)
        if ts.size:
            days.update(np.unique(
                (ts // 86400).astype(np.int64)).tolist())
    return {str(np.datetime64(int(d), "D")) for d in days}


def _sql(base, extra):
    chunk, _r, _n, keys = LAYOUT[base]
    cols = ["sensor", *keys, "t0", "t1", "n", "times"] + list(extra)
    return (f"INSERT INTO {chunk} ({', '.join(cols)}) "
            f"VALUES ({', '.join('?' * len(cols))})")


class ChunkAppender:
    """Buffer captures and flush them as whole chunks, as compact_db would.

    Appends only: chunks already in the table are never rewritten, so a resumed
    run costs nothing for the days it already has. The final partial chunk is
    written on exit, which leaves one short chunk per run -- harmless to read,
    and a later `compact_db.py` pass tidies it.
    """

    def __init__(self, con, base, sensor, payload_col, key=None):
        self.con, self.base, self.sensor = con, base, sensor
        self.key = () if key is None else (key,)
        self.limit = LAYOUT[base][2]
        self.sql = _sql(base, (payload_col,))
        self.ts, self.buf, self.total = [], [], 0

    def add(self, times, payloads):
        """`times` epoch seconds, `payloads` int8 arrays, one per capture."""
        for t, p in zip(times, payloads):
            self.ts.append(float(t))
            self.buf.append(p.tobytes() if hasattr(p, "tobytes") else p)
            if len(self.buf) >= self.limit:
                self.flush()

    def flush(self):
        if not self.buf:
            return
        ts = np.array(self.ts, dtype=np.float64)
        self.con.execute(self.sql, [
            self.sensor, *self.key, float(ts[0]), float(ts[-1]), len(self.buf),
            zlib.compress(ts.tobytes(), Z),
            zlib.compress(b"".join(self.buf), Z)])
        self.total += len(self.buf)
        self.ts, self.buf = [], []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if exc[0] is None:
            self.flush()
        return False


def stored_span(con, base, sensor, kind):
    """(t_min, t_max, captures) for this sensor, whichever shape is on disk."""
    chunk, rows, _n, _k = LAYOUT[base]
    if kind == "chunk":
        r = con.execute(f"SELECT min(t0), max(t1), coalesce(sum(n), 0) "
                        f"FROM {chunk} WHERE sensor=?", [sensor]).fetchone()
    else:
        r = con.execute(f"SELECT min(t), max(t), count(*) "
                        f"FROM {rows} WHERE sensor=?", [sensor]).fetchone()
    return r[0], r[1], r[2] or 0
