# Padova Transit Punctuality

A data pipeline that measures the punctuality of public transport in Padova (Italy) by comparing GTFS static schedules against GTFS-Realtime vehicle data.

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

## Data sources

All feeds are published by **Busitalia Veneto** as open data under Italy's IODL 2.0 licence.

The project currently targets the **tram** feeds (publicly accessible). Bus feeds require HTTP Basic Auth and are a possible later extension.

| Feed | Format | URL |
|------|--------|-----|
| Tram schedule | GTFS (zip) | `https://gtfs-biv.fsbusitalia.com/GTFS-BIV-TRAM/gtfs-biv-tram.zip` |
| Tram trip updates | GTFS-RT (protobuf) | `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-trip-updates.pb` |
| Tram vehicle positions | GTFS-RT (protobuf) | `https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-vehicle-positions.pb` |
| Tram schedule archive | GTFS (zip) | `https://gtfs-biv.fsbusitalia.com/GTFS-BIV-TRAM-HISTORY/` |

## Stack

Python 3.12 | Apache Airflow | Parquet | DuckDB | dbt | Streamlit | GitHub Actions

Optional cloud variant: Google Cloud Storage | BigQuery

## Running locally

Prerequisites: Docker and Docker Compose.

```bash
# 1. Clone the repo
git clone https://github.com/DavideFornari/padova-transit-punctuality.git
cd padova-transit-punctuality

# 2. Create your .env from the template and fill in secrets
cp .env.example .env

# 3. Start Airflow (webserver + scheduler + Postgres)
make up

# 4. Open the Airflow UI and unpause the DAGs
#    http://localhost:8080  (admin / admin)

# 5. Once the DAGs have collected data: build the warehouse and open the dashboard
make dbt-run
make dbt-test
make dashboard

# 6. Stop everything
make down
```

### Development

Requires **Python 3.12** — the same version as the Airflow image and CI. Airflow 2.10.5 does not support 3.13+.

```bash
# Create .venv and install the project with dev extras
make venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Run linter and tests
make lint
make test

# Auto-format
make fmt

# Install pre-commit hooks
make pre-commit-install
```

Dependencies live in `pyproject.toml` only — there is no separate requirements file to keep in sync. `make venv` and CI install from it the same way: Airflow first under [its own constraint file](https://github.com/apache/airflow/blob/constraints-2.10.5/constraints-3.12.txt) (plain `pip install apache-airflow` is not reliably resolvable), then `pip install -e ".[dev]"` on top. Add the `cloud` extra for the optional cloud variant: `pip install -e ".[dev,cloud]"`.

## Cloud variant (optional)

The pipeline runs entirely on a laptop with no cloud account. Setting a few environment variables lights up a cloud path on top of it, without changing a line of SQL:

```
                          ┌─ local Parquet ──────────────┐
Ingestion DAGs ───────────┤                              ├──> DuckDB + dbt ──> marts ──> Streamlit
                          └─ gs://<bucket>/… (mirrored) ─┘                        │
                                                                                  └──> BigQuery (published)
```

Three independent switches, each a no-op while unset:

| What | Enabled by | Effect |
|------|-----------|--------|
| Raw zone in GCS | `GCS_BUCKET` | Ingestion DAGs mirror every Parquet file to the bucket, preserving the `date=/hour=` and `version=` partition layout |
| dbt reads from GCS | `DBT_TARGET=cloud` + `GCS_HMAC_KEY_ID` / `GCS_HMAC_SECRET` | Staging models read `gs://…` over DuckDB's httpfs extension instead of the local disk |
| Marts in BigQuery | `GCP_PROJECT` | `make publish-bq` loads `fct_stop_events` and the dimensions into BigQuery for BI tools |

```bash
# Install the optional dependencies
pip install -e ".[cloud]"

# 1. Authenticate for GCS uploads (or set GOOGLE_APPLICATION_CREDENTIALS)
gcloud auth application-default login

# 2. Fill in the cloud section of .env, then run the DAGs as usual —
#    files land locally and in gs://<bucket>/ with identical keys.

# 3. Build the warehouse from the bucket instead of the local disk
DBT_TARGET=cloud make dbt-run

# 4. Publish the marts to BigQuery
make publish-bq
```

### Design decision: why the warehouse stays on DuckDB

The obvious reading of "cloud variant" is a full BigQuery port of the dbt project. That was considered and rejected:

- **Full BigQuery port** — external tables over GCS plus cross-database macros so every model runs on either adapter. Every staging model is DuckDB SQL (`strptime`, `to_timestamp`, `::date`, and `INTERVAL` arithmetic in `gtfs_time_to_interval` for GTFS times past midnight), so this means rewriting all twelve models and maintaining two dialects of the trickiest logic in the project — for a dataset of a single tram network that DuckDB handles comfortably.
- **GCS only** — mirror the raw zone and stop there. Cheap, but nothing downstream ever demonstrates a cloud warehouse.
- **Hybrid (chosen)** — GCS is the raw zone, DuckDB reads it directly over `httpfs` so the models stay in one dialect, and the finished marts are published to BigQuery where BI tools expect them. Cloud storage and cloud warehouse are both real, and the local path stays byte-for-byte the same code.

The publish step exports each mart to Parquet and loads it with `WRITE_TRUNCATE`, so re-running it replaces table contents rather than appending duplicates — the same idempotency rule the ingestion DAGs follow.

If the data ever outgrew DuckDB, the migration path is the first option above, and the `read_source()` macro is already the single seam where the source location is decided.

### Known limits

- Staging models rescan every raw Parquet file on each `dbt run`. At tram volume (thousands of rows/day) this stays fast for a long time; the designed fix, when needed, is converting `stg_trip_updates` to an incremental model filtered on the `date=` Hive partition.
- Freshness checks read only the newest files — deterministic filenames sort chronologically — so monitoring cost stays flat as history grows.
- The cloud path is verified up to authentication — unit-tested object keys and publish logic, both dbt targets compile, and a cloud-target run reaches Google's storage API (rejected only for lack of real credentials). It has not yet been exercised against a live bucket.

## Project status

**All 7 milestones complete.** Latest addition: the optional cloud variant — GCS raw-zone mirroring, a DuckDB-over-GCS dbt target, and BigQuery mart publishing.

A full-project review (2026-07-31) found nine issues — three correctness bugs in the fact table (timezone handling, schedule-version tie-breaks, skipped-stop delays), the rest operational and robustness fixes — all resolved; see `CLAUDE.md` for the closed-out list.

## License

[MIT](LICENSE)
