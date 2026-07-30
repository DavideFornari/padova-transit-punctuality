"""DAG: check data freshness and fail loudly on stale feeds.

Runs every 10 minutes.  Checks that both RT feeds (trip updates and
vehicle positions) have recent data, and that the static GTFS schedule
has been downloaded within the last 14 days.  A StaleDataError causes
the task (and DAG run) to fail, which surfaces in the Airflow UI and
can trigger alerting.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from padova_transit.quality.freshness import check_rt_freshness, check_static_freshness

DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))


@dag(
    dag_id="check_freshness",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "monitoring"],
    doc_md=__doc__,
)
def check_freshness():
    @task()
    def check_trip_updates():
        check_rt_freshness(
            base_dir=DATA_DIR,
            feed_name="trip_updates",
            now=datetime.now(),
            max_age=timedelta(minutes=10),
        )

    @task()
    def check_vehicle_positions():
        check_rt_freshness(
            base_dir=DATA_DIR,
            feed_name="vehicle_positions",
            now=datetime.now(),
            max_age=timedelta(minutes=10),
        )

    @task()
    def check_static():
        check_static_freshness(
            base_dir=DATA_DIR,
            now=datetime.now(),
            max_age=timedelta(days=14),
        )

    # All three checks run in parallel.
    check_trip_updates()
    check_vehicle_positions()
    check_static()


check_freshness()
