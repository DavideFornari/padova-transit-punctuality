"""DAG: ingest tram GTFS static schedule weekly.

Downloads the GTFS zip, hashes it, and stores a new version only if the
content has changed.  Each version is written as Parquet files under
data/gtfs_static/version=<hash>/, with a manifest tracking validity windows.

When GCS_BUCKET is set, the new version directory and the manifest are also
mirrored to Cloud Storage.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from padova_transit.cloud.gcs import mirror_directory_to_gcs, upload_to_gcs
from padova_transit.constants import GTFS_STATIC_TRAM_URL
from padova_transit.ingest.static import ingest_static_gtfs, manifest_path

DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))


@dag(
    dag_id="ingest_static",
    schedule="0 4 * * 1",  # Every Monday at 04:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["ingest", "static", "tram"],
    doc_md=__doc__,
)
def ingest_static():
    @task()
    def download_and_version(**context):
        logical_date: datetime = context["logical_date"]
        result = ingest_static_gtfs(
            url=GTFS_STATIC_TRAM_URL,
            base_dir=DATA_DIR,
            now=logical_date,
        )
        if result.get("status") == "ingested":
            # Only the new version directory is uploaded — earlier versions are
            # immutable and already in the bucket.  The manifest is rewritten on
            # every ingest, so it always goes back up.
            mirror_directory_to_gcs(Path(result["version_dir"]), DATA_DIR)
            upload_to_gcs(manifest_path(DATA_DIR), DATA_DIR)
        return result

    download_and_version()


ingest_static()
