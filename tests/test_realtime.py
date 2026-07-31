"""Tests for GTFS-RT ingestion: decode and write logic (no network calls)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from google.transit import gtfs_realtime_pb2 as rt

from padova_transit.ingest.realtime import (
    TRIP_UPDATES_SCHEMA,
    VEHICLE_POSITIONS_SCHEMA,
    _output_path,
    decode_trip_updates,
    decode_vehicle_positions,
    write_parquet,
)

# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_trip_update_feed() -> rt.FeedMessage:
    """Build a minimal trip-updates FeedMessage in memory."""
    feed = rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1700000000

    entity = feed.entity.add()
    entity.id = "entity-1"
    tu = entity.trip_update
    tu.trip.trip_id = "trip-42"
    tu.trip.route_id = "route-T"
    tu.trip.direction_id = 0
    tu.trip.start_date = "20260730"
    tu.trip.start_time = "08:15:00"
    tu.vehicle.id = "tram-01"
    tu.vehicle.label = "Tram 01"
    tu.timestamp = 1700000000

    stu = tu.stop_time_update.add()
    stu.stop_sequence = 1
    stu.stop_id = "stop-A"
    stu.arrival.delay = 30
    stu.arrival.time = 1700000030
    stu.departure.delay = 45
    stu.departure.time = 1700000045

    stu2 = tu.stop_time_update.add()
    stu2.stop_sequence = 2
    stu2.stop_id = "stop-B"
    stu2.arrival.delay = -10
    stu2.arrival.time = 1700000200

    return feed


def _make_vehicle_positions_feed() -> rt.FeedMessage:
    """Build a minimal vehicle-positions FeedMessage in memory."""
    feed = rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1700000000

    entity = feed.entity.add()
    entity.id = "entity-v1"
    vp = entity.vehicle
    vp.trip.trip_id = "trip-42"
    vp.trip.route_id = "route-T"
    vp.trip.direction_id = 1
    vp.trip.start_date = "20260730"
    vp.trip.start_time = "08:15:00"
    vp.vehicle.id = "tram-01"
    vp.vehicle.label = "Tram 01"
    vp.position.latitude = 45.4064
    vp.position.longitude = 11.8768
    vp.position.bearing = 180.0
    vp.position.speed = 12.5
    vp.current_stop_sequence = 3
    vp.stop_id = "stop-C"
    vp.current_status = rt.VehiclePosition.IN_TRANSIT_TO
    vp.timestamp = 1700000000

    return feed


# ── Decode tests ─────────────────────────────────────────────────────────────


def test_decode_trip_updates_row_count() -> None:
    rows = decode_trip_updates(_make_trip_update_feed())
    assert len(rows) == 2  # one entity with two stop_time_updates


def test_decode_trip_updates_fields() -> None:
    rows = decode_trip_updates(_make_trip_update_feed())
    row = rows[0]
    assert row["feed_timestamp"] == 1700000000
    assert row["trip_id"] == "trip-42"
    assert row["stop_id"] == "stop-A"
    assert row["arrival_delay"] == 30
    assert row["vehicle_id"] == "tram-01"

    # stu2 has no departure block at all — must decode as None, not 0.
    assert rows[1]["departure_delay"] is None
    assert rows[1]["departure_time"] is None


def test_decode_skipped_stop_yields_none_not_zero() -> None:
    """A SKIPPED stop with no arrival/departure must not decode as delay 0."""
    feed = rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1700000000
    entity = feed.entity.add()
    entity.id = "entity-skip"
    tu = entity.trip_update
    tu.trip.trip_id = "trip-42"
    stu = tu.stop_time_update.add()
    stu.stop_sequence = 7
    stu.stop_id = "stop-X"
    stu.schedule_relationship = 1  # SKIPPED

    rows = decode_trip_updates(feed)
    assert rows[0]["arrival_delay"] is None
    assert rows[0]["departure_delay"] is None
    assert rows[0]["schedule_relationship"] == 1


def test_decode_vehicle_positions_row_count() -> None:
    rows = decode_vehicle_positions(_make_vehicle_positions_feed())
    assert len(rows) == 1


def test_decode_vehicle_positions_fields() -> None:
    row = decode_vehicle_positions(_make_vehicle_positions_feed())[0]
    assert row["feed_timestamp"] == 1700000000
    assert row["trip_id"] == "trip-42"
    assert row["latitude"] == pytest.approx(45.4064, abs=1e-4)
    assert row["speed"] == 12.5
    assert row["current_status"] == rt.VehiclePosition.IN_TRANSIT_TO


def test_decode_empty_feed() -> None:
    feed = rt.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1700000000
    assert decode_trip_updates(feed) == []
    assert decode_vehicle_positions(feed) == []


# ── Write tests ──────────────────────────────────────────────────────────────


def test_write_parquet_creates_file(tmp_path: Path) -> None:
    rows = decode_trip_updates(_make_trip_update_feed())
    out = tmp_path / "test.parquet"
    result = write_parquet(rows, TRIP_UPDATES_SCHEMA, out)
    assert result == out
    assert out.exists()
    table = pq.read_table(out)
    assert table.num_rows == 2
    assert table.schema.equals(TRIP_UPDATES_SCHEMA)


def test_write_parquet_empty_returns_none(tmp_path: Path) -> None:
    out = tmp_path / "empty.parquet"
    result = write_parquet([], TRIP_UPDATES_SCHEMA, out)
    assert result is None
    assert not out.exists()


# ── Idempotency ──────────────────────────────────────────────────────────────


def test_output_path_is_deterministic() -> None:
    dt = datetime(2026, 7, 30, 14, 25, 0, tzinfo=UTC)
    base = Path("/data")
    p1 = _output_path(base, "trip_updates", dt)
    p2 = _output_path(base, "trip_updates", dt)
    assert p1 == p2
    assert "date=2026-07-30" in str(p1)
    assert "hour=14" in str(p1)
    assert p1.name == "20260730T142500.parquet"


def test_overwrite_idempotency(tmp_path: Path) -> None:
    """Writing the same data twice to the same path overwrites — no duplication."""
    rows = decode_vehicle_positions(_make_vehicle_positions_feed())
    out = tmp_path / "vp.parquet"
    write_parquet(rows, VEHICLE_POSITIONS_SCHEMA, out)
    write_parquet(rows, VEHICLE_POSITIONS_SCHEMA, out)
    table = pq.read_table(out)
    assert table.num_rows == 1  # still 1, not 2
