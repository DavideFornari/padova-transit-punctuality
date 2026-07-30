"""Fetch, decode, and persist GTFS-Realtime feeds as Parquet files.

All functions are pure (no Airflow imports) so they can be tested and reused
outside of a DAG context.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)

# ── Schemas ──────────────────────────────────────────────────────────────────

TRIP_UPDATES_SCHEMA = pa.schema(
    [
        ("feed_timestamp", pa.int64()),
        ("entity_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.uint8()),
        ("start_date", pa.string()),
        ("start_time", pa.string()),
        ("vehicle_id", pa.string()),
        ("vehicle_label", pa.string()),
        ("stop_sequence", pa.uint32()),
        ("stop_id", pa.string()),
        ("arrival_delay", pa.int32()),
        ("arrival_time", pa.int64()),
        ("departure_delay", pa.int32()),
        ("departure_time", pa.int64()),
        ("schedule_relationship", pa.uint8()),
    ]
)

VEHICLE_POSITIONS_SCHEMA = pa.schema(
    [
        ("feed_timestamp", pa.int64()),
        ("entity_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.uint8()),
        ("start_date", pa.string()),
        ("start_time", pa.string()),
        ("vehicle_id", pa.string()),
        ("vehicle_label", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("bearing", pa.float32()),
        ("speed", pa.float32()),
        ("current_stop_sequence", pa.uint32()),
        ("stop_id", pa.string()),
        ("current_status", pa.uint8()),
        ("timestamp", pa.int64()),
    ]
)


# ── Fetch ────────────────────────────────────────────────────────────────────


def fetch_feed(url: str, *, timeout: int = 30) -> gtfs_realtime_pb2.FeedMessage:
    """Download and parse a GTFS-RT protobuf feed."""
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


# ── Decode ───────────────────────────────────────────────────────────────────


def decode_trip_updates(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    """Flatten a trip-updates feed into one row per stop_time_update."""
    rows: list[dict] = []
    ts = feed.header.timestamp
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        vehicle = tu.vehicle
        for stu in tu.stop_time_update:
            rows.append(
                {
                    "feed_timestamp": ts,
                    "entity_id": entity.id,
                    "trip_id": trip.trip_id,
                    "route_id": trip.route_id,
                    "direction_id": trip.direction_id,
                    "start_date": trip.start_date,
                    "start_time": trip.start_time,
                    "vehicle_id": vehicle.id,
                    "vehicle_label": vehicle.label,
                    "stop_sequence": stu.stop_sequence,
                    "stop_id": stu.stop_id,
                    "arrival_delay": stu.arrival.delay,
                    "arrival_time": stu.arrival.time,
                    "departure_delay": stu.departure.delay,
                    "departure_time": stu.departure.time,
                    "schedule_relationship": stu.schedule_relationship,
                }
            )
    return rows


def decode_vehicle_positions(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    """Flatten a vehicle-positions feed into one row per entity."""
    rows: list[dict] = []
    ts = feed.header.timestamp
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vp = entity.vehicle
        trip = vp.trip
        vehicle = vp.vehicle
        pos = vp.position
        rows.append(
            {
                "feed_timestamp": ts,
                "entity_id": entity.id,
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "direction_id": trip.direction_id,
                "start_date": trip.start_date,
                "start_time": trip.start_time,
                "vehicle_id": vehicle.id,
                "vehicle_label": vehicle.label,
                "latitude": pos.latitude,
                "longitude": pos.longitude,
                "bearing": pos.bearing,
                "speed": pos.speed,
                "current_stop_sequence": vp.current_stop_sequence,
                "stop_id": vp.stop_id,
                "current_status": vp.current_status,
                "timestamp": vp.timestamp,
            }
        )
    return rows


# ── Persist ──────────────────────────────────────────────────────────────────


def write_parquet(
    rows: list[dict],
    schema: pa.Schema,
    path: Path,
) -> Path | None:
    """Write rows to a Parquet file. Returns the path written, or None if empty.

    An empty feed (no entities) is normal outside service hours — we skip
    writing rather than creating a zero-row file, but still log it.
    """
    if not rows:
        logger.info("No rows to write — feed was empty, skipping %s", path.name)
        return None

    table = pa.Table.from_pylist(rows, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    logger.info("Wrote %d rows to %s", len(rows), path)
    return path


# ── High-level entry point ───────────────────────────────────────────────────


def _output_path(base_dir: Path, feed_name: str, logical_date: datetime) -> Path:
    """Build a deterministic output path for idempotent writes.

    Layout: <base_dir>/<feed_name>/date=YYYY-MM-DD/hour=HH/<YYYYMMDDTHHMMSS>.parquet

    Using the logical execution timestamp (truncated to the second) as the
    filename means re-running the same DAG interval overwrites the same file
    — no duplicates.
    """
    return (
        base_dir
        / feed_name
        / f"date={logical_date.strftime('%Y-%m-%d')}"
        / f"hour={logical_date.strftime('%H')}"
        / f"{logical_date.strftime('%Y%m%dT%H%M%S')}.parquet"
    )


def ingest_trip_updates(
    url: str,
    base_dir: Path,
    logical_date: datetime,
) -> Path | None:
    """Fetch trip-updates feed, decode, and write to Parquet."""
    feed = fetch_feed(url)
    rows = decode_trip_updates(feed)
    path = _output_path(base_dir, "trip_updates", logical_date)
    return write_parquet(rows, TRIP_UPDATES_SCHEMA, path)


def ingest_vehicle_positions(
    url: str,
    base_dir: Path,
    logical_date: datetime,
) -> Path | None:
    """Fetch vehicle-positions feed, decode, and write to Parquet."""
    feed = fetch_feed(url)
    rows = decode_vehicle_positions(feed)
    path = _output_path(base_dir, "vehicle_positions", logical_date)
    return write_parquet(rows, VEHICLE_POSITIONS_SCHEMA, path)
