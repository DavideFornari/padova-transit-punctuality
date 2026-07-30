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

## Running locally

Prerequisites: Docker and Docker Compose.

```bash
# 1. Clone the repo
git clone https://github.com/<your-username>/padova-transit-punctuality.git
cd padova-transit-punctuality

# 2. Create your .env from the template and fill in secrets
cp .env.example .env

# 3. Start Airflow (webserver + scheduler + Postgres)
make up

# 4. Open the Airflow UI
#    http://localhost:8080  (admin / admin)

# 5. Stop everything
make down
```

### Development

```bash
# Install dev dependencies (in a virtualenv)
pip install -e ".[dev]"

# Run linter and tests
make lint
make test

# Auto-format
make fmt

# Install pre-commit hooks
make pre-commit-install
```

## Project status

**Milestone 4 of 7** — Warehouse and dbt models (staging, dimensions, fact table with delay analysis). Milestones 1-3 (scaffolding, real-time ingestion, static schedule ingestion) are complete.

## License

[MIT](LICENSE)
