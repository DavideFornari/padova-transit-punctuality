# Project brief — Padova transit punctuality pipeline

## Context

I'm a data engineer building a portfolio project to demonstrate production data engineering
skills for job applications. The repository will be public on GitHub and read by recruiters
and technical interviewers, so code quality, documentation and repo hygiene matter as much as
functionality.

## What we're building

A data pipeline that measures the punctuality of public transport in Padova (Italy) by
comparing the scheduled timetable against real-time vehicle data.

### Data sources — Busitalia Veneto open GTFS feeds

Static schedule (GTFS, zip, updated roughly weekly):
- Bus: `https://gtfs-biv.fsbusitalia.com/GTFS-BIV/gtfs-biv.zip`
- Tram: `https://gtfs-biv.fsbusitalia.com/GTFS-BIV-TRAM/gtfs-biv-tram.zip`
- Archives of past versions: `https://gtfs-biv.fsbusitalia.com/GTFS-BIV-HISTORY/`

Real-time (GTFS-Realtime, protobuf, refreshed continuously):
- Bus trip updates: `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV/start-gtfs-rt-trip-updates-fc.pb`
- Bus vehicle positions: `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV/start-gtfs-rt-vehicle-positions-fc.pb`
- Tram trip updates: `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-trip-updates.pb`
- Tram vehicle positions: `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-vehicle-positions.pb`

Start with bus only. Tram is a later extension.

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

## Scope for this session

**Milestone 1 only.** Do not write ingestion logic, DAG business logic, dbt models or the
dashboard yet.

Deliverables:

- Repository layout with clear separation: `dags/`, `src/` (importable package), `dbt/`,
  `tests/`, `docker/`, `docs/`.
- `docker-compose.yml` bringing up Airflow (webserver, scheduler, its Postgres metadata DB)
  with the local `dags/` and `src/` mounted. Pin image versions — no `latest` tags.
- Dependency management with pinned versions. Use `pyproject.toml`.
- `.env.example` with documented variables. Never commit a real `.env`.
- `Makefile` with at least: `make up`, `make down`, `make lint`, `make test`, `make fmt`.
- `ruff` and `pre-commit` configured.
- One trivial passing test in `tests/`, so CI has something real to run.
- GitHub Actions workflow running lint and tests on push and pull request.
- `.gitignore` covering Python, Docker, dbt, DuckDB files, Parquet output and `.env`.
- A `README.md` with: what the project does, the architecture diagram above, the data sources
  with attribution, how to run it locally, and the current status (milestone 1 of 7).
- An MIT `LICENSE`.

Definition of done: on a clean machine, `make up` starts Airflow and the UI is reachable, and
`make lint` and `make test` both pass.

## Rules

- Explain design decisions briefly as you go — I want to understand the choices, not just receive files.
- Ask me before anything destructive: force pushes, history rewrites, deleting files I wrote.
- Never commit data files, credentials, or `.env`. Raw feed data stays out of git entirely.
- Commit messages in imperative mood, one logical change per commit.
- Keep the local-only path working at every milestone. No step may require a cloud account.
- Code, comments, docstrings and documentation in English.
