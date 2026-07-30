"""DAG: ingest tram GTFS-Realtime feeds every minute.

Polls trip-updates and vehicle-positions feeds in parallel, decodes the
protobuf payloads, and writes the results as Parquet files partitioned by
date and hour. Idempotent — re-running for the same interval overwrites
the same output file.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

from padova_transit.constants import (
    GTFSRT_TRAM_TRIP_UPDATES_URL,
    GTFSRT_TRAM_VEHICLE_POSITIONS_URL,
)
from padova_transit.ingest.realtime import ingest_trip_updates, ingest_vehicle_positions

DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))


@dag(
    dag_id="ingest_realtime",
    schedule="* * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingest", "realtime", "tram"],
    doc_md=__doc__,
)
def ingest_realtime():
    @task()
    def fetch_trip_updates(**context):
        logical_date: datetime = context["logical_date"]
        result = ingest_trip_updates(
            url=GTFSRT_TRAM_TRIP_UPDATES_URL,
            base_dir=DATA_DIR,
            logical_date=logical_date,
        )
        return str(result) if result else "empty"

    @task()
    def fetch_vehicle_positions(**context):
        logical_date: datetime = context["logical_date"]
        result = ingest_vehicle_positions(
            url=GTFSRT_TRAM_VEHICLE_POSITIONS_URL,
            base_dir=DATA_DIR,
            logical_date=logical_date,
        )
        return str(result) if result else "empty"

    # Run both fetches in parallel (no dependency between them).
    fetch_trip_updates()
    fetch_vehicle_positions()


ingest_realtime()
