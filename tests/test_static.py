"""Tests for GTFS static schedule ingestion (no network calls)."""

from __future__ import annotations

import io
import zipfile
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq

from padova_transit.ingest.static import (
    extract_and_convert,
    hash_bytes,
    ingest_static_gtfs,
    read_manifest,
    version_exists,
)

# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_gtfs_zip(
    calendar_dates: str | None = None,
    stops: str | None = None,
    routes: str | None = None,
    trips: str | None = None,
    stop_times: str | None = None,
) -> bytes:
    """Build a minimal GTFS zip archive in memory."""
    if calendar_dates is None:
        calendar_dates = (
            "service_id,date,exception_type\n1,20260730,1\n1,20260731,1\n2,20260801,1\n"
        )
    if stops is None:
        stops = (
            "stop_id,stop_name,stop_lat,stop_lon\n"
            "S1,Stazione,45.4175,11.8809\n"
            "S2,Pontevigodarzere,45.4340,11.8720\n"
        )
    if routes is None:
        routes = "route_id,route_short_name,route_type\nT1,SIR1,0\n"
    if trips is None:
        trips = "route_id,service_id,trip_id,direction_id\nT1,1,trip-1,0\nT1,2,trip-2,1\n"
    if stop_times is None:
        stop_times = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "trip-1,08:00:00,08:01:00,S1,1\n"
            "trip-1,08:10:00,08:11:00,S2,2\n"
        )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("calendar_dates.txt", calendar_dates)
        zf.writestr("stops.txt", stops)
        zf.writestr("routes.txt", routes)
        zf.writestr("trips.txt", trips)
        zf.writestr("stop_times.txt", stop_times)
    return buf.getvalue()


# ── Unit tests ───────────────────────────────────────────────────────────────


def test_hash_bytes_deterministic() -> None:
    data = b"hello world"
    assert hash_bytes(data) == hash_bytes(data)


def test_hash_bytes_changes_with_content() -> None:
    assert hash_bytes(b"a") != hash_bytes(b"b")


def test_extract_and_convert_creates_parquet(tmp_path: Path) -> None:
    zip_bytes = _make_gtfs_zip()
    version_hash = hash_bytes(zip_bytes)
    version_dir, valid_from, valid_to = extract_and_convert(zip_bytes, tmp_path, version_hash)

    assert version_dir.exists()
    assert (version_dir / "stops.parquet").exists()
    assert (version_dir / "routes.parquet").exists()
    assert (version_dir / "trips.parquet").exists()
    assert (version_dir / "stop_times.parquet").exists()
    assert (version_dir / "calendar_dates.parquet").exists()


def test_extract_validity_window(tmp_path: Path) -> None:
    zip_bytes = _make_gtfs_zip()
    version_hash = hash_bytes(zip_bytes)
    _, valid_from, valid_to = extract_and_convert(zip_bytes, tmp_path, version_hash)

    assert valid_from == "20260730"
    assert valid_to == "20260801"


def test_extract_parquet_row_counts(tmp_path: Path) -> None:
    zip_bytes = _make_gtfs_zip()
    version_hash = hash_bytes(zip_bytes)
    version_dir, _, _ = extract_and_convert(zip_bytes, tmp_path, version_hash)

    assert pq.read_table(version_dir / "stops.parquet").num_rows == 2
    assert pq.read_table(version_dir / "trips.parquet").num_rows == 2
    assert pq.read_table(version_dir / "stop_times.parquet").num_rows == 2
    assert pq.read_table(version_dir / "calendar_dates.parquet").num_rows == 3


def test_manifest_empty_initially(tmp_path: Path) -> None:
    assert read_manifest(tmp_path) == []


def test_version_exists_false_initially(tmp_path: Path) -> None:
    assert version_exists(tmp_path, "abc123") is False


# ── Integration (still no network) ──────────────────────────────────────────


def test_ingest_writes_manifest(tmp_path: Path, monkeypatch) -> None:
    """Full ingest flow with a patched fetch."""
    zip_bytes = _make_gtfs_zip()

    monkeypatch.setattr(
        "padova_transit.ingest.static.fetch_gtfs_zip",
        lambda url, **kw: zip_bytes,
    )

    result = ingest_static_gtfs(
        url="http://fake",
        base_dir=tmp_path,
        now=datetime(2026, 7, 30, 4, 0),
    )

    assert result["status"] == "ingested"
    assert result["valid_from"] == "20260730"
    assert result["valid_to"] == "20260801"

    manifest = read_manifest(tmp_path)
    assert len(manifest) == 1
    assert manifest[0]["version"] == result["version"]


def test_ingest_skips_duplicate(tmp_path: Path, monkeypatch) -> None:
    """Second ingest of the same zip is a no-op."""
    zip_bytes = _make_gtfs_zip()

    monkeypatch.setattr(
        "padova_transit.ingest.static.fetch_gtfs_zip",
        lambda url, **kw: zip_bytes,
    )

    first = ingest_static_gtfs(url="http://fake", base_dir=tmp_path)
    second = ingest_static_gtfs(url="http://fake", base_dir=tmp_path)

    assert first["status"] == "ingested"
    assert second["status"] == "skipped"

    # Manifest still has exactly one entry
    assert len(read_manifest(tmp_path)) == 1


def test_ingest_stores_new_version(tmp_path: Path, monkeypatch) -> None:
    """A changed zip creates a second version."""
    zip_v1 = _make_gtfs_zip()
    zip_v2 = _make_gtfs_zip(
        calendar_dates=("service_id,date,exception_type\n1,20260805,1\n1,20260806,1\n")
    )

    call_count = 0

    def fake_fetch(url, **kw):
        nonlocal call_count
        call_count += 1
        return zip_v1 if call_count == 1 else zip_v2

    monkeypatch.setattr("padova_transit.ingest.static.fetch_gtfs_zip", fake_fetch)

    r1 = ingest_static_gtfs(url="http://fake", base_dir=tmp_path)
    r2 = ingest_static_gtfs(url="http://fake", base_dir=tmp_path)

    assert r1["status"] == "ingested"
    assert r2["status"] == "ingested"
    assert r1["version"] != r2["version"]

    manifest = read_manifest(tmp_path)
    assert len(manifest) == 2
