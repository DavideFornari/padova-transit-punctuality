"""Tests for the optional cloud variant — no network or GCP calls are made.

The GCS client and the BigQuery client are both replaced with fakes, so these
tests verify the wiring (object keys, skip-when-disabled, idempotent load
config) rather than the vendor SDKs.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from padova_transit.cloud import gcs
from padova_transit.cloud.bigquery import (
    export_table_to_parquet,
    is_bigquery_enabled,
    load_parquet_to_bigquery,
    publish_marts,
)
from padova_transit.cloud.gcs import (
    blob_name,
    is_cloud_enabled,
    mirror_directory_to_gcs,
    upload_to_gcs,
)

# ── Fakes ────────────────────────────────────────────────────────────────────


class FakeBlob:
    def __init__(self, name: str, uploads: list[tuple[str, str]]) -> None:
        self.name = name
        self._uploads = uploads

    def upload_from_filename(self, filename: str) -> None:
        self._uploads.append((self.name, filename))


class FakeBucket:
    def __init__(self, name: str, uploads: list[tuple[str, str]]) -> None:
        self.name = name
        self._uploads = uploads

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(name, self._uploads)


class FakeStorageClient:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str]] = []

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(name, self.uploads)


@pytest.fixture
def fake_gcs(monkeypatch):
    """Replace the cached GCS client with a fake that records uploads."""
    client = FakeStorageClient()
    monkeypatch.setattr(gcs, "_client", lambda: client)
    return client


# ── GCS: enable/disable ──────────────────────────────────────────────────────


def test_cloud_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    assert is_cloud_enabled() is False


def test_cloud_enabled_when_bucket_set(monkeypatch) -> None:
    monkeypatch.setenv("GCS_BUCKET", "my-bucket")
    assert is_cloud_enabled() is True


def test_upload_returns_none_when_disabled(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    f = tmp_path / "test.parquet"
    f.write_bytes(b"fake")
    assert upload_to_gcs(f, tmp_path) is None


def test_mirror_returns_empty_when_disabled(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    assert mirror_directory_to_gcs(tmp_path, tmp_path) == []


# ── GCS: object keys ─────────────────────────────────────────────────────────


def test_blob_name_preserves_hive_partitions(tmp_path: Path) -> None:
    """The partition directories must survive, or the gs:// glob finds nothing."""
    local = tmp_path / "trip_updates" / "date=2026-07-30" / "hour=08" / "x.parquet"
    assert blob_name(local, tmp_path) == "trip_updates/date=2026-07-30/hour=08/x.parquet"


def test_blob_name_rejects_path_outside_data_dir(tmp_path: Path) -> None:
    outside = tmp_path.parent / "elsewhere.parquet"
    with pytest.raises(ValueError):
        blob_name(outside, tmp_path)


def test_upload_uses_mirrored_key(monkeypatch, tmp_path: Path, fake_gcs) -> None:
    monkeypatch.setenv("GCS_BUCKET", "my-bucket")
    local = tmp_path / "vehicle_positions" / "date=2026-07-30" / "hour=08" / "x.parquet"
    local.parent.mkdir(parents=True)
    local.write_bytes(b"fake")

    uri = upload_to_gcs(local, tmp_path)

    key = "vehicle_positions/date=2026-07-30/hour=08/x.parquet"
    assert uri == f"gs://my-bucket/{key}"
    assert fake_gcs.uploads == [(key, str(local))]


def test_mirror_directory_uploads_tree(monkeypatch, tmp_path: Path, fake_gcs) -> None:
    monkeypatch.setenv("GCS_BUCKET", "my-bucket")
    version_dir = tmp_path / "gtfs_static" / "version=abc123"
    version_dir.mkdir(parents=True)
    (version_dir / "stops.parquet").write_bytes(b"fake")
    (version_dir / "routes.parquet").write_bytes(b"fake")
    (tmp_path / "gtfs_static" / "_versions.parquet").write_bytes(b"fake")

    uris = mirror_directory_to_gcs(version_dir, tmp_path)

    # Only the version directory is mirrored — not the sibling manifest.
    assert uris == [
        "gs://my-bucket/gtfs_static/version=abc123/routes.parquet",
        "gs://my-bucket/gtfs_static/version=abc123/stops.parquet",
    ]


# ── BigQuery publish ─────────────────────────────────────────────────────────


def test_bigquery_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    assert is_bigquery_enabled() is False


def test_publish_marts_skipped_when_not_configured(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    assert publish_marts(tmp_path / "warehouse.duckdb") == {}


def test_export_table_to_parquet(tmp_path: Path) -> None:
    con = duckdb.connect()
    con.execute("create table dim_stops as select 1 as stop_id, 'Prato' as stop_name")

    out = export_table_to_parquet(con, "dim_stops", tmp_path)

    assert out.exists()
    rows = duckdb.connect().execute(f"select * from read_parquet('{out.as_posix()}')").fetchall()
    assert rows == [(1, "Prato")]


def test_load_parquet_uses_truncating_load(monkeypatch, tmp_path: Path) -> None:
    """WRITE_TRUNCATE is what makes re-publishing idempotent rather than doubling rows."""
    bigquery = pytest.importorskip("google.cloud.bigquery")

    parquet = tmp_path / "dim_stops.parquet"
    duckdb.connect().execute(
        f"copy (select 1 as stop_id) to '{parquet.as_posix()}' (format parquet)"
    )

    captured = {}

    class FakeJob:
        output_rows = 1

        def result(self):
            return None

    class FakeBQClient:
        def load_table_from_file(self, fh, table_id, job_config):
            captured["table_id"] = table_id
            captured["disposition"] = job_config.write_disposition
            captured["format"] = job_config.source_format
            return FakeJob()

    rows = load_parquet_to_bigquery(parquet, "proj.ds.dim_stops", FakeBQClient())

    assert rows == 1
    assert captured["table_id"] == "proj.ds.dim_stops"
    assert captured["disposition"] == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert captured["format"] == bigquery.SourceFormat.PARQUET


def test_publish_marts_round_trip(monkeypatch, tmp_path: Path) -> None:
    """Every mart is exported from DuckDB and loaded under its fully qualified id."""
    pytest.importorskip("google.cloud.bigquery")
    monkeypatch.setenv("GCP_PROJECT", "my-project")
    monkeypatch.setenv("BQ_DATASET", "padova_transit")

    warehouse = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(warehouse))
    for table in ("dim_stops", "dim_routes", "dim_trips", "fct_stop_events"):
        con.execute(f"create table {table} as select 1 as id, '{table}' as source_table")
    con.close()

    loaded_tables: list[str] = []
    created_datasets: list[str] = []

    class FakeJob:
        output_rows = 1

        def result(self):
            return None

    class FakeBQClient:
        def create_dataset(self, dataset, exists_ok=False):
            created_datasets.append(str(dataset.dataset_id))
            return dataset

        def load_table_from_file(self, fh, table_id, job_config):
            # The file must still be readable at load time, not already cleaned up.
            assert fh.read(4) == b"PAR1"
            loaded_tables.append(table_id)
            return FakeJob()

    loaded = publish_marts(warehouse, client=FakeBQClient())

    assert created_datasets == ["padova_transit"]
    assert loaded == dict.fromkeys(("dim_stops", "dim_routes", "dim_trips", "fct_stop_events"), 1)
    assert loaded_tables == [
        "my-project.padova_transit.dim_stops",
        "my-project.padova_transit.dim_routes",
        "my-project.padova_transit.dim_trips",
        "my-project.padova_transit.fct_stop_events",
    ]
