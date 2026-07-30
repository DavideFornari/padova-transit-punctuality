# Project brief — Padova transit punctuality pipeline

## What we're building

A data pipeline that measures the punctuality of public transport in Padova (Italy) by
comparing the scheduled timetable against real-time vehicle data.

### Data sources — Busitalia Veneto open GTFS feeds

Primary sources — **tram** (publicly accessible, no auth required):
- Static schedule: `https://gtfs-biv.fsbusitalia.com/GTFS-BIV-TRAM/gtfs-biv-tram.zip`
- Trip updates: `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-trip-updates.pb`
- Vehicle positions: `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-vehicle-positions.pb`
- Historical archive: `https://gtfs-biv.fsbusitalia.com/GTFS-BIV-TRAM-HISTORY/`

Secondary sources — **bus** (require HTTP Basic Auth, possible later extension pending open-data access request):
- Static schedule: `https://gtfs-biv.fsbusitalia.com/GTFS-BIV/gtfs-biv.zip`
- Trip updates: `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV/start-gtfs-rt-trip-updates-fc.pb`
- Vehicle positions: `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV/start-gtfs-rt-vehicle-positions-fc.pb`
- Historical archive: `https://gtfs-biv.fsbusitalia.com/GTFS-BIV-HISTORY/`

The project targets the tram feeds. Bus feeds are behind HTTP Basic Auth and are a
possible later extension, pending an open-data access request.

### Architecture

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

### Stack

- Python 3.12
- Apache Airflow for orchestration, running via Docker Compose
- `gtfs-realtime-bindings` for protobuf decoding
- Parquet for the raw landing zone
- DuckDB as the analytical engine
- dbt (dbt-duckdb adapter) for transformations and tests
- Streamlit for the dashboard
- GitHub Actions for CI
- `ruff` for linting and formatting, `pytest` for tests, `pre-commit` for hooks

## Roadmap

1. **Scaffolding** — repo structure, Docker Compose with Airflow, tooling, CI. No pipeline logic.
2. **Real-time ingestion** — DAG polling the two `.pb` feeds every minute, decoding to Parquet.
   Must be idempotent: re-running for the same interval must not duplicate rows.
3. **Static schedule ingestion** — weekly download, *versioned*. This is the hard part: trip IDs
   can change between feed versions, so a real-time event from a given day must be joined against
   the schedule that was valid on that day, not the current one.
4. **Warehouse and models** — dbt staging models, dimensions for stops/routes/trips, a fact table
   of stop events with scheduled vs actual time and the resulting delay. Must correctly handle
   GTFS times past midnight (e.g. `25:30:00` belongs to the previous service day) and Europe/Rome
   timezone with DST.
5. **Data quality** — dbt tests, source freshness checks, a DAG that fails loudly if a feed goes stale.
6. **Dashboard** — Streamlit: punctuality by route, by time of day, by stop.
7. **Optional cloud variant** — the same DAGs writing to GCS and BigQuery. Only after the local
   version works end to end. The local version must always remain runnable with no cloud account.

## Progress

- [x] **Milestone 1 — Scaffolding** — repo structure, Docker Compose, CI, tooling.
- [x] **Milestone 2 — Real-time ingestion** — DAG polling tram trip-updates and vehicle-positions
      every minute, decoding protobuf to Parquet. Idempotent via deterministic file paths.
- [x] **Milestone 3 — Static schedule ingestion** — weekly DAG downloading the GTFS zip,
      SHA-256 versioning, Parquet conversion with a manifest tracking validity windows.
- [x] **Milestone 4 — Warehouse and models** — dbt staging models, dimensions, fact table
      with version-aware schedule joins, midnight-crossing handling, Europe/Rome timezone.
- [x] **Milestone 5 — Data quality** — expanded dbt tests (relationships, not_null, delay
      bounds), freshness DAG checking RT and static feed staleness every 10 minutes.
- [x] **Milestone 6 — Dashboard** — Streamlit app with tabs for punctuality by route,
      time of day, stop (with map), and delay distribution histogram.
- [x] **Milestone 7 — Optional cloud variant** — ingestion DAGs mirror the Parquet raw zone
      to GCS, a `cloud` dbt target reads it back over DuckDB httpfs, and the marts are
      published to BigQuery. All three switches are off unless their env vars are set.

## Open follow-ups — cloud variant verification

The cloud path is implemented and merged, but it has never touched a real bucket.
Verified so far, on 2026-07-30: unit tests cover the GCS object keys and the BigQuery
publish; both dbt targets compile; `dbt debug --target cloud` passes; and a `dbt run`
on the cloud target reaches Google over the network — it fails with `HTTP 403` from
`storage.googleapis.com/<bucket>/gtfs_static/_versions.parquet`, which is the expected
answer to a request signed with fake HMAC credentials. Everything up to authentication
is therefore known to work.

What is still unproven, in the order it should be done:

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

Unrelated to the cloud path, worth knowing: DuckDB 1.5.5 could not load the `httpfs`
extension on the development machine — Windows application control blocked the binary.
DuckDB 1.2.1, the version this project pins, loads it fine.

## Rules

- Explain design decisions briefly as you go — I want to understand the choices, not just receive files.
- Ask me before anything destructive: force pushes, history rewrites, deleting files I wrote.
- Never commit data files, credentials, or `.env`. Raw feed data stays out of git entirely.
- Commit messages in imperative mood, one logical change per commit.
- Keep the local-only path working at every milestone. No step may require a cloud account.
- Code, comments, docstrings and documentation in English.
