"""Publish the dbt marts from DuckDB to BigQuery.

Optional — a no-op unless ``GCP_PROJECT`` is set.  This is the second half
of the cloud variant: GCS holds the raw Parquet zone, DuckDB still runs the
transformations (see the design note in the README), and the finished marts
are copied into BigQuery so a BI tool can query them.

Each table is exported to a temporary Parquet file and loaded with
WRITE_TRUNCATE, which makes the publish step idempotent: re-running it
replaces the table contents rather than appending duplicates.  Parquet is
used as the transfer format rather than pandas so that types survive the
round trip without dtype guessing.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# The marts published to BigQuery — dimensions plus the stop-event fact table.
MART_TABLES = ("dim_stops", "dim_routes", "dim_trips", "fct_stop_events")

DEFAULT_DATASET = "padova_transit"
DEFAULT_LOCATION = "europe-west1"


def is_bigquery_enabled() -> bool:
    """Return True if BigQuery publishing is configured."""
    return bool(os.getenv("GCP_PROJECT"))


def export_table_to_parquet(con, table: str, out_dir: Path) -> Path:
    """Export one DuckDB table to a Parquet file in ``out_dir``."""
    out_path = out_dir / f"{table}.parquet"
    # Identifier is from MART_TABLES, not user input, so quoting is enough.
    con.execute(f"copy \"{table}\" to '{out_path.as_posix()}' (format parquet)")
    return out_path


def load_parquet_to_bigquery(parquet_path: Path, table_id: str, client) -> int:
    """Load a Parquet file into a BigQuery table, replacing its contents.

    Returns the number of rows in the table after the load.
    """
    from google.cloud import bigquery  # lazy import — not installed in local mode

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with parquet_path.open("rb") as fh:
        job = client.load_table_from_file(fh, table_id, job_config=job_config)
    job.result()  # wait for completion; raises on failure

    logger.info("Loaded %d rows into %s", job.output_rows, table_id)
    return job.output_rows


def publish_marts(
    duckdb_path: Path,
    *,
    project: str | None = None,
    dataset: str | None = None,
    location: str | None = None,
    tables: tuple[str, ...] = MART_TABLES,
    client=None,
) -> dict[str, int]:
    """Copy the dbt marts from a DuckDB file into BigQuery.

    Returns a mapping of table name to rows loaded, or an empty dict when
    BigQuery is not configured.  The dataset is created if it doesn't exist.
    """
    project = project or os.getenv("GCP_PROJECT")
    if not project:
        logger.info("GCP_PROJECT not set — skipping BigQuery publish")
        return {}

    dataset = dataset or os.getenv("BQ_DATASET", DEFAULT_DATASET)
    location = location or os.getenv("BQ_LOCATION", DEFAULT_LOCATION)

    import duckdb
    from google.cloud import bigquery  # lazy import — not installed in local mode

    if client is None:
        client = bigquery.Client(project=project)

    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    dataset_ref.location = location
    client.create_dataset(dataset_ref, exists_ok=True)

    loaded: dict[str, int] = {}
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            for table in tables:
                parquet_path = export_table_to_parquet(con, table, Path(tmp))
                table_id = f"{project}.{dataset}.{table}"
                loaded[table] = load_parquet_to_bigquery(parquet_path, table_id, client)
    finally:
        con.close()

    return loaded


def main() -> None:
    """CLI entry point: ``python -m padova_transit.cloud.bigquery``."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=Path(os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")),
        help="Path to the DuckDB warehouse built by dbt",
    )
    parser.add_argument("--project", default=None, help="GCP project (default: $GCP_PROJECT)")
    parser.add_argument("--dataset", default=None, help="BigQuery dataset (default: $BQ_DATASET)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not is_bigquery_enabled() and not args.project:
        parser.error("GCP_PROJECT is not set — nothing to publish to")

    loaded = publish_marts(args.duckdb_path, project=args.project, dataset=args.dataset)
    for table, rows in loaded.items():
        print(f"{table}: {rows} rows")


if __name__ == "__main__":
    main()
