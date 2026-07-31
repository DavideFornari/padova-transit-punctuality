# CLAUDE.md — Padova Transit Punctuality

A data pipeline measuring the punctuality of public transport in Padova (Italy) by
comparing the scheduled GTFS timetable against real-time GTFS-RT vehicle data.
This is a **public portfolio repo** read by recruiters and technical interviewers —
code quality, documentation and repo hygiene matter as much as functionality.

## Architecture

```
GTFS static (weekly)  ─┐
GTFS-RT trip updates  ─┼─> Ingestion (Python + Airflow, Docker)
GTFS-RT positions     ─┘         │
                                 v
                        Raw zone (Parquet, partitioned by date/hour)
                                 │
                                 v
                        Warehouse (DuckDB + dbt models + tests)
                                 │
                                 v
                        Dashboard (Streamlit)
```

**Stack:** Python 3.12 · Airflow 2.10.5 (Docker Compose) · gtfs-realtime-bindings ·
Parquet · DuckDB · dbt-duckdb · Streamlit · GitHub Actions · ruff / pytest / pre-commit.
Optional cloud variant: GCS raw-zone mirror + BigQuery mart publishing.

**Data sources** — Busitalia Veneto open GTFS feeds (IODL 2.0). The project targets
the **tram** feeds, which are public. **Bus** feeds exist but sit behind HTTP Basic
Auth — a possible later extension, pending an open-data access request; the auth
env vars are already wired (unused) in `.env.example`. All feed URLs live in
`src/padova_transit/constants.py` (single source of truth; also tabled in README).

## Repo map

```
dags/                     Airflow DAGs — thin glue only, logic lives in src/
src/padova_transit/
  ingest/                 fetch/decode/persist for RT (realtime.py) and static (static.py)
  quality/freshness.py    staleness checks used by the check_freshness DAG
  cloud/                  optional GCS mirror (gcs.py) + BigQuery publish (bigquery.py)
  dashboard/              Streamlit app.py + SQL in queries.py
dbt/                      staging (views over Parquet) + marts (dims, fct_stop_events)
tests/                    unit tests + dbt integration tests (conftest.py builds a
                          synthetic warehouse) + Airflow DagBag tests (test_dags.py)
docker/, docker-compose.yml   local Airflow stack (`make up`)
```

## Rules

- Explain design decisions briefly as you go — the user wants to understand the
  choices, not just receive files.
- Ask before anything destructive: force pushes, history rewrites, deleting files
  the user wrote.
- Never commit data files, credentials, or `.env`. Raw feed data stays out of git.
- Commit messages in imperative mood, one logical change per commit.
- The local-only path must keep working with **no cloud account**: every cloud
  feature is a no-op unless its env var (`GCS_BUCKET`, `DBT_TARGET`, `GCP_PROJECT`) is set.
- Code, comments, docstrings and documentation in English.

## Environment & verification gate

The Python 3.12 virtualenv lives in `.venv/` (create with `make venv`; Airflow
2.10.5 does not support Python 3.13+). Run after **every** change:

```bash
# Windows (this machine). On POSIX use .venv/bin/ instead of .venv/Scripts/.
.venv/Scripts/python -m pytest tests/ -q                       # all must pass
.venv/Scripts/python -m ruff check src/ dags/ tests/           # must be clean
.venv/Scripts/python -m ruff format --check src/ dags/ tests/  # must be clean
```

The pytest suite includes dbt integration tests (`tests/test_dbt_models.py` builds a
real warehouse from synthetic data in `tests/conftest.py`) and Airflow DagBag tests
(`tests/test_dags.py`), so it exercises SQL and DAG changes too.

## Status — all 7 milestones complete

Each milestone established an invariant that must not regress:

1. **Scaffolding** — repo structure, Docker Compose, CI, tooling.
2. **Real-time ingestion** — minute-level DAG decoding both `.pb` feeds to Parquet.
   *Invariant: idempotent — re-running an interval overwrites the same file, never duplicates.*
3. **Static schedule ingestion** — weekly download, SHA-256 versioned, manifest with
   validity windows. *Invariant: trip IDs change between feed versions, so an RT event
   must join the schedule version valid on ITS day, never simply the newest one.*
4. **Warehouse** — dbt staging + dims + `fct_stop_events`. *Invariant: GTFS times past
   midnight (`25:30:00` belongs to the previous service day) and Europe/Rome time
   must be handled correctly.*
5. **Data quality** — dbt tests + freshness DAG. *Invariant: a stale feed fails loudly.*
6. **Dashboard** — Streamlit: punctuality by route, time of day, stop; delay distribution.
7. **Cloud variant (optional)** — DAGs mirror the raw zone to GCS, a `cloud` dbt target
   reads it back over DuckDB httpfs, marts publish to BigQuery. *Invariant: all three
   switches off by default; the local path never requires a cloud account.*

## Open follow-ups — cloud variant verification

The cloud path is implemented and merged, but it has never touched a real bucket.
Verified so far (2026-07-30): unit tests cover the GCS object keys and the BigQuery
publish; both dbt targets compile; `dbt debug --target cloud` passes; and a `dbt run`
on the cloud target reaches Google over the network — it fails with `HTTP 403` from
`storage.googleapis.com/<bucket>/gtfs_static/_versions.parquet`, the expected answer
to a request signed with fake HMAC credentials. Everything up to authentication is
known to work.

Still unproven, in the order it should be done:

- [ ] Provision a GCS bucket and an HMAC key pair (Cloud Storage > Settings >
      Interoperability); set `GCS_BUCKET`, `GCS_HMAC_KEY_ID`, `GCS_HMAC_SECRET` in `.env`.
- [ ] Run the ingestion DAGs with `GCS_BUCKET` set. Confirm objects land under keys that
      keep their partitions: `trip_updates/date=…/hour=…/*.parquet` and
      `gtfs_static/version=…/*.parquet`, plus `gtfs_static/_versions.parquet`.
- [ ] Run `DBT_TARGET=cloud make dbt-run` against the real bucket. This is the riskiest
      step: it is the first time Hive partition globbing (`version=*`, `date=*/hour=*`) is
      exercised over GCS rather than a local disk, so confirm `version`, `date` and `hour`
      come back as columns and the row counts match the local warehouse.
- [ ] Run `make publish-bq`, then run it a second time — row counts must stay identical,
      since `WRITE_TRUNCATE` is what makes the publish idempotent.
- [ ] Optional, needs no GCP account: stand up MinIO locally as an S3-compatible endpoint
      and point DuckDB at it with an endpoint override, to exercise partition globbing and
      remote Parquet reads. Does not cover the `gcs` secret type against Google itself.

---

# Improvement backlog

Verified findings from a full-project review (2026-07-31). Work top to bottom —
tasks are ordered by severity. **One commit per task**, and check the task off (or
delete it) in this file in the same commit that implements it. Each task lists the
exact edits; quote blocks marked OLD must match the file exactly (they did on
2026-07-31).

## Task 1 — DONE (2026-07-31): fct timestamps are now Europe/Rome wall-clock, independent of session timezone

## Task 2 — GTFS version choice is arbitrary when validity windows overlap  [CORRECTNESS, HIGH]

**File:** `dbt/models/marts/fct_stop_events.sql`

**Problem (verified):** weekly downloads of a feed valid for months mean overlapping
validity windows are the NORMAL case. Each RT row then joins several versions, and
the `row_number()` orders only by `tu.feed_timestamp` — which is identical across
those fanned-out copies. DuckDB returns an arbitrary one as `rn = 1`, so delays may
be computed against a stale schedule. Deterministic fix: prefer the newest version.

**Edit 1.** OLD:

```sql
        row_number() over (
            partition by tu.service_date, tu.trip_id, tu.stop_sequence
            order by tu.feed_timestamp desc
        ) as rn
```

NEW:

```sql
        row_number() over (
            partition by tu.service_date, tu.trip_id, tu.stop_sequence
            order by tu.feed_timestamp desc, v.downloaded_at desc
        ) as rn
```

**Edit 2 — header comment.** OLD:

```sql
--   1. Version matching: RT start_date is matched to the GTFS static version
--      whose validity window (from calendar_dates.txt) covers that date.
```

NEW:

```sql
--   1. Version matching: RT start_date is matched to the GTFS static version
--      whose validity window (from calendar_dates.txt) covers that date.
--      Weekly downloads overlap, so several versions can cover one date —
--      ties break to the most recently downloaded version.
```

**Test to add.** In `tests/conftest.py`, make the synthetic data contain TWO
overlapping versions so the tie-break is actually exercised:

1. Replace the single-row `_versions.parquet` write. OLD:

```python
    pq.write_table(
        pa.table(
            {
                "version": ["aabbccdd1122eeff"],
                "downloaded_at": ["2026-07-28T04:00:00"],
                "valid_from": ["20260728"],
                "valid_to": ["20260803"],
            }
        ),
        static_dir / "_versions.parquet",
    )
```

NEW (the old version now starts earlier; a newer overlapping version is added):

```python
    # Two versions with overlapping validity windows — the normal case for
    # weekly downloads.  fct_stop_events must pick the newer one (by
    # downloaded_at) for service dates covered by both.
    pq.write_table(
        pa.table(
            {
                "version": ["aabbccdd1122eeff", "ffeeddccbba91234"],
                "downloaded_at": ["2026-07-21T04:00:00", "2026-07-28T04:00:00"],
                "valid_from": ["20260721", "20260728"],
                "valid_to": ["20260803", "20260810"],
            }
        ),
        static_dir / "_versions.parquet",
    )
```

2. After the five `pq.write_table(...)` calls that populate
`ver_dir = static_dir / "version=aabbccdd1122"`, add a second version directory
`ver_dir2 = static_dir / "version=ffeeddccbba9"` containing **all five files**
(`stops`, `routes`, `trips`, `stop_times`, `calendar_dates`) with identical
content, EXCEPT `stop_times.parquet` where trip-1/seq-1 moves 5 minutes later:
`arrival_time` `"08:05:00"` and `departure_time` `"08:06:00"` (rows 2 and 3
unchanged). The easiest way is to copy-paste the five existing writes and change
the directory and those two strings. If `test_fct_stop_events_row_count` fails
afterwards, you forgot one of the five files — an incomplete version directory
makes the schedule join drop rows.

3. Add to `tests/test_dbt_models.py`:

```python
def test_overlapping_versions_pick_newest_schedule(dbt_warehouse: str) -> None:
    """Two versions cover 2026-07-30; the newer download must win the join."""
    con = duckdb.connect(dbt_warehouse)
    version, minute = con.sql("""
        select gtfs_version, extract(minute from scheduled_arrival)
        from fct_stop_events
        where trip_id = 'trip-1' and stop_sequence = 1
    """).fetchone()
    assert version == "ffeeddccbba9"
    assert minute == 5
```

Note: if Task 1 is not done yet, `minute` still works — the minute is unaffected
by the timezone bug. Existing test expectations do not change.

## Task 3 — skipped stops decode as "perfectly on time"  [CORRECTNESS, HIGH]

**Problem (verified):** protobuf returns `0` for absent fields. A `SKIPPED`
stop_time_update with no arrival block decodes as `arrival_delay = 0`, enters the
fact table, and inflates punctuality. Additionally, NULL delays that reach the
dashboard's histogram CASE expression fall into the `else` branch and get counted
as "> 10 min late".

**Edit 1 —** `src/padova_transit/ingest/realtime.py`, in `decode_trip_updates`. OLD:

```python
        for stu in tu.stop_time_update:
            rows.append(
                {
```

NEW:

```python
        for stu in tu.stop_time_update:
            # Absent protobuf fields read as 0, indistinguishable from a genuine
            # zero-second delay — store None when the event is not present.
            arrival = stu.arrival if stu.HasField("arrival") else None
            departure = stu.departure if stu.HasField("departure") else None
            rows.append(
                {
```

Then replace the four event fields inside the dict. OLD:

```python
                    "arrival_delay": stu.arrival.delay,
                    "arrival_time": stu.arrival.time,
                    "departure_delay": stu.departure.delay,
                    "departure_time": stu.departure.time,
```

NEW:

```python
                    "arrival_delay": arrival.delay
                    if arrival is not None and arrival.HasField("delay")
                    else None,
                    "arrival_time": arrival.time
                    if arrival is not None and arrival.HasField("time")
                    else None,
                    "departure_delay": departure.delay
                    if departure is not None and departure.HasField("delay")
                    else None,
                    "departure_time": departure.time
                    if departure is not None and departure.HasField("time")
                    else None,
```

(Run `ruff format` afterwards; it may reflow these.)

**Edit 2 —** `dbt/models/staging/stg_trip_updates.sql`: add `schedule_relationship,`
to the select list (after `stop_id,`), and add a WHERE clause after the `from` line:

```sql
-- SCHEDULED (0) only: SKIPPED (1) and NO_DATA (2) updates carry no usable
-- delay and must not become punctuality events.  The raw Parquet keeps them.
where schedule_relationship = 0
```

**Edit 3 —** `dbt/models/marts/fct_stop_events.sql`, in the `with_schedule` CTE,
after the `inner join {{ ref('stg_stop_times') }} st ...` lines, add:

```sql
    -- A row with neither delay carries no punctuality information.
    where d.arrival_delay is not null or d.departure_delay is not null
```

**Edit 4 —** `dbt/models/marts/_marts.yml`: the two delay columns are now nullable
(a stop can report only arrival or only departure). Remove `tests: [not_null]` from
`arrival_delay_seconds` and `departure_delay_seconds` (keep the descriptions; add
"NULL when the feed omitted this event" to each).

**Edit 5 —** `src/padova_transit/dashboard/queries.py`: NULL arrival delays must not
hit aggregates or the histogram. Add `where arrival_delay_seconds is not null`
(with the `f.` prefix where the query aliases the table) to all four queries:
`punctuality_by_route`, `punctuality_by_hour`, `punctuality_by_stop` (these three:
so `pct_on_time`'s `count(*)` denominator excludes NULLs), and `delay_distribution`
(otherwise NULL falls into the `else '> 10 min (late)'` bucket).

**Tests.** In `tests/test_realtime.py` add (module already imports
`gtfs_realtime_pb2 as rt` and `decode_trip_updates`):

```python
def test_decode_skipped_stop_yields_none_not_zero() -> None:
    """A SKIPPED stop with no arrival/departure must not decode as delay 0."""
    feed = rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1700000000
    entity = feed.entity.add()
    entity.id = "entity-skip"
    tu = entity.trip_update
    tu.trip.trip_id = "trip-42"
    stu = tu.stop_time_update.add()
    stu.stop_sequence = 7
    stu.stop_id = "stop-X"
    stu.schedule_relationship = 1  # SKIPPED

    rows = decode_trip_updates(feed)
    assert rows[0]["arrival_delay"] is None
    assert rows[0]["departure_delay"] is None
    assert rows[0]["schedule_relationship"] == 1
```

Also: the existing helper `_make_trip_update_feed` has a second stop (`stu2`) with
no departure block — add these to the existing decode test:

```python
    assert rows[1]["departure_delay"] is None
    assert rows[1]["departure_time"] is None
```

## Task 4 — freshness check false-alarms all night and reads all history  [OPERATIONAL, MEDIUM]

**File:** `src/padova_transit/quality/freshness.py` (and `dags/check_freshness.py`,
`tests/test_freshness.py`)

**Problem:** (a) the docstring of `check_rt_freshness` promises "outside service
hours we skip the check" but no such code exists — once any data exists, every
nightly run raises `StaleDataError`, training operators to ignore the alert.
(b) `latest_rt_feed_timestamp` reads EVERY Parquet file ever written (~2,880/day at
minute cadence) on every 10-minute check. (c) naive datetimes make the math depend
on the host timezone.

**Edit 1 —** rewrite `latest_rt_feed_timestamp`. Filenames are UTC timestamps, so
lexicographic descending sort of the full glob = newest first; empty feeds never
write files, so the newest file has rows:

```python
def latest_rt_feed_timestamp(base_dir: Path, feed_name: str) -> int | None:
    """Return the max feed_timestamp from the newest Parquet files of a RT feed.

    Filenames embed the UTC poll timestamp, so sorting the glob descending puts
    the newest file first — only a handful of files are read, not the history.
    Returns None if no files exist (first deploy, or outside service hours).
    """
    pattern = str(base_dir / feed_name / "date=*" / "hour=*" / "*.parquet")
    files = sorted(glob.glob(pattern), reverse=True)
    for f in files[:5]:
        table = pq.read_table(f, columns=["feed_timestamp"])
        if table.num_rows > 0:
            return max(table.column("feed_timestamp").to_pylist())
    return None
```

Move `import glob` to the module's top-level imports. Add to the imports:
`from datetime import UTC, datetime, timedelta` and `from zoneinfo import ZoneInfo`.

**Edit 2 —** module-level constants and quiet-hours gate in `check_rt_freshness`:

```python
ROME = ZoneInfo("Europe/Rome")
# The tram does not run roughly 00:00-05:00 Europe/Rome; a freshness alarm in
# that window would be a permanent nightly false positive.
QUIET_HOURS = range(0, 5)
```

At the top of `check_rt_freshness` (docstring: state that `now` must be
timezone-aware):

```python
    if now.astimezone(ROME).hour in QUIET_HOURS:
        logger.info("Quiet hours in Europe/Rome — skipping %s freshness check", feed_name)
        return
```

And make the comparison timezone-aware. OLD: `feed_time = datetime.fromtimestamp(ts)`
NEW: `feed_time = datetime.fromtimestamp(ts, tz=UTC)`.

**Edit 3 —** `latest_static_download` currently mixes naive and aware datetimes
(`downloaded_at` written by Airflow includes an offset; test fixtures do not).
After parsing, normalise: OLD `return max(datetime.fromisoformat(d) for d in dates)`
NEW:

```python
    parsed = [datetime.fromisoformat(d) for d in dates]
    return max(d if d.tzinfo else d.replace(tzinfo=UTC) for d in parsed)
```

**Edit 4 —** `dags/check_freshness.py`: change all three `now=datetime.now()` to
`now=datetime.now(UTC)` and import `UTC` from `datetime`.

**Edit 5 —** `tests/test_freshness.py`: make every `now=datetime(...)` argument
timezone-aware (`tzinfo=UTC`) and choose hours whose Europe/Rome equivalent is
OUTSIDE 00:00–05:00 (e.g. 10:00 UTC) so existing stale/fresh assertions still
trigger. Add one new test:

```python
def test_rt_freshness_skipped_during_quiet_hours(tmp_path: Path) -> None:
    """02:30 Europe/Rome (00:30 UTC in summer): stale data must NOT raise."""
    now = datetime(2026, 7, 30, 0, 30, tzinfo=UTC)
    check_rt_freshness(tmp_path, "trip_updates", now=now)  # must not raise
```

## Task 5 — no retries on network-bound DAG tasks  [OPERATIONAL, MEDIUM]

**Files:** `dags/ingest_realtime.py`, `dags/ingest_static.py`

A single transient feed error fails the run; at one poll per minute that is a
certainty. In both files: extend the datetime import to
`from datetime import datetime, timedelta`, and add one argument to the `@dag(...)`
decorator call (after `catchup=False,`):

```python
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
```

Do NOT add retries inside `fetch_feed`/`fetch_gtfs_zip` — retrying at both layers
multiplies attempts. `tests/test_dags.py` will confirm the DAGs still parse.

## Task 6 — the ./src volume mount does nothing  [OPERATIONAL, MEDIUM]

**File:** `docker/airflow.Dockerfile`

`docker-compose.yml` mounts `./src` into the container, but the image installs the
package NON-editable, so the scheduler imports the copy baked into site-packages
and host edits never take effect — a silently broken dev loop. Fix: OLD
`RUN pip install --no-cache-dir /opt/airflow/project` NEW
`RUN pip install --no-cache-dir -e /opt/airflow/project`. With the editable
install resolving to `/opt/airflow/project/src` — exactly where compose mounts the
host's `src/` — live code reload works. Add a comment saying so.

## Task 7 — dashboard connection blocks dbt writes  [OPERATIONAL, LOW]

**File:** `src/padova_transit/dashboard/app.py`

`@st.cache_resource` keeps one read-only DuckDB connection alive for the app's
lifetime; DuckDB writers need exclusivity, so `make dbt-run` fails with a lock
error while the dashboard is open. Fix: delete the `@st.cache_resource` line and
add a docstring to `get_connection`:

```python
def get_connection():
    """A fresh read-only connection per rerun.

    Deliberately not cached: a cached connection would hold a read lock on the
    DuckDB file for the app's lifetime, blocking `dbt run` from replacing the
    warehouse.  Connecting costs milliseconds per rerun.
    """
    return duckdb.connect(DUCKDB_PATH, read_only=True)
```

## Task 8 — empty-string crashes in strptime  [ROBUSTNESS, LOW]

`strptime('', '%Y%m%d')` is a runtime error that kills the whole model. Guard the
fields that are optional in the source data with `nullif`:

- `dbt/models/staging/stg_trip_updates.sql` and `stg_vehicle_positions.sql`:
  OLD `strptime(start_date, '%Y%m%d')::date as service_date`
  NEW `strptime(nullif(start_date, ''), '%Y%m%d')::date as service_date`
- `dbt/models/marts/fct_stop_events.sql` join: wrap both bounds —
  OLD `strptime(v.valid_from, '%Y%m%d')::date` / `strptime(v.valid_to, '%Y%m%d')::date`
  NEW `strptime(nullif(v.valid_from, ''), '%Y%m%d')::date` /
  `strptime(nullif(v.valid_to, ''), '%Y%m%d')::date`

(NULL service_date rows simply drop out of the inner join — correct behaviour.)

## Task 9 — dbt test fixture can leak onto the cloud target  [ROBUSTNESS, LOW]

**File:** `tests/conftest.py`

The fixture doesn't pin the dbt target, so `DBT_TARGET=cloud` exported in a shell
silently runs the integration suite against the cloud profile. In the
`runner.invoke([...])` list, after `"run",` add two elements: `"--target", "dev",`.

## Task 10 — DONE (2026-07-31): README status line updated

## Task 11 — dangling dbt sources  [OPTIONAL — decide with the user]

`dbt/models/staging/_sources.yml` declares eight source tables that no model
references via `source()` (staging models call `read_parquet()` directly), so dbt
lineage is decorative. Options: (a) leave as documentation — the header comment
already explains this; (b) wire dbt-duckdb `external_location` on each source and
switch staging models to `{{ source('raw', ...) }}`. Do NOT start (b) without
asking the user — it touches all eight staging models for cosmetic benefit.

## Task 12 — document freshness cost in README  [DOCS, LOW — only after Task 4]

Partially done 2026-07-31: README already has a "Known limits" section (rescan
bullet + cloud-verification status). Once Task 4 is implemented, add this bullet
to that section — it is FALSE until then:

```markdown
- Freshness checks read only the newest files — deterministic filenames sort
  chronologically — so monitoring cost stays flat as history grows.
```

Also update the "cloud path … has not yet been exercised against a live bucket"
bullet in that section whenever the Open follow-ups checklist above gets done.

---

# Verified-working facts (do not re-derive)

- Both dbt targets compile; `dbt debug --target cloud` passes; a cloud-target run
  reaches `storage.googleapis.com` and fails only on fake credentials (HTTP 403).
  Live-bucket verification: see "Open follow-ups" above.
- DuckDB 1.2.1 (pinned) loads `httpfs` on this machine; the system-wide DuckDB
  1.5.5 cannot (Windows application control blocks that binary). Use `.venv`.
- Airflow 2.10.5 does not support Python 3.13+; the venv must stay on 3.12.
- `pip install apache-airflow` needs Airflow's constraint file, installed FIRST,
  then `pip install -e .` on top (see Makefile `venv` target). One step cannot work:
  project pins for duckdb/pyarrow/ruff deliberately differ from the constraints.
