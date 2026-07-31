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

Once this checklist is done, update the "cloud path … has not yet been exercised against
a live bucket" bullet in the README's "Known limits" section — it is the same status
recorded twice, and both must move together.

---

# Hardening pass (2026-07-31) — closed

A full-project review on 2026-07-31 found nine bugs plus one documentation gap.
All nine are fixed, one commit each; nothing below is actionable. Full detail —
the exact OLD/NEW edits and the empirical proof each fix actually worked — lives
in the corresponding commit messages on `main` (search for "Fix" / "Add" dated
2026-07-31), not repeated here.

- **fct timestamps depended on the machine's timezone.** `(x::timestamp)::timestamptz`
  read the DuckDB *session* timezone; the same row produced `scheduled_arrival`
  08:00 on a Rome machine, 10:00 in Docker (UTC). Fixed by keeping all
  `fct_stop_events` timestamps as naive Europe/Rome wall-clock — GTFS times are
  agency-local by definition, so no conversion was ever needed.
- **GTFS version choice was arbitrary when validity windows overlap.** Weekly
  downloads make overlap the normal case; the dedup `row_number()` tied on
  `feed_timestamp` alone, identical across fanned-out version matches. Fixed by
  breaking ties on `downloaded_at desc` (prefer the newest download).
- **Skipped stops decoded as "perfectly on time."** Protobuf's zero-default for
  absent fields made a SKIPPED stop's delay indistinguishable from a genuine
  zero-second delay. Fixed by storing NULL when `HasField()` is false, filtering
  non-SCHEDULED updates in staging, and excluding NULL delays from dashboard
  aggregates and the histogram.
- **Freshness check false-alarmed all night and rescanned all history.** No
  quiet-hours gate existed despite the docstring claiming one; every 10-minute
  check also read every RT Parquet file ever written. Fixed with a 00:00–05:00
  Europe/Rome gate and a bounded scan of only the newest files (filenames sort
  chronologically).
- **No retries on network-bound DAG tasks.** Added `retries=2, retry_delay=30s`
  to both ingest DAGs (Airflow layer only, not inside the fetch functions).
- **The `./src` volume mount did nothing.** The Airflow image installed the
  package non-editable, so host edits never reached the running scheduler.
  Switched to `pip install -e`.
- **Dashboard connection blocked `dbt run`.** Removed `@st.cache_resource` on the
  DuckDB connection, which held a lock for the app's whole lifetime.
- **Empty-string `strptime` crashes.** Wrapped the optional date fields
  (`start_date`, `valid_from`, `valid_to`) in `nullif(x, '')`.
- **dbt test fixture could leak onto the cloud target.** Pinned `--target dev`
  in the fixture's `dbt run` invocation.

**Decided, not fixed:** `dbt/models/staging/_sources.yml` declares eight source
tables no model actually references via `source()` — staging models read through
the `read_source()` macro instead, so dbt's lineage graph is decorative. Discussed
with the user 2026-07-31: left as documentation. Wiring it up via
`external_location` + `{{ source('raw', ...) }}` would touch all eight staging
models for benefits (accurate `dbt docs` lineage, source-freshness checks) the
project doesn't currently need. Revisit only if either becomes a real need.

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
