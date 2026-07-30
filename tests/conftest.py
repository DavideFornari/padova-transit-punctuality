"""Shared fixtures for integration tests that need a DuckDB warehouse."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"


def _create_synthetic_data(data_dir: Path) -> None:
    """Create a minimal data directory with Parquet files for dbt to read."""
    from padova_transit.ingest.realtime import (
        TRIP_UPDATES_SCHEMA,
        VEHICLE_POSITIONS_SCHEMA,
    )

    # ── GTFS static version ──────────────────────────────────────────────
    static_dir = data_dir / "gtfs_static"
    static_dir.mkdir(parents=True)

    pq.write_table(
        pa.table(
            {
                "version": ["aabbccdd1122eeff"],
                "downloaded_at": ["2026-07-28T04:00:00"],
                "valid_from": ["20260728"],
                "valid_to": ["20260803"],
            }
        ),
        static_dir / "_versions.parquet",
    )

    ver_dir = static_dir / "version=aabbccdd1122"
    ver_dir.mkdir()

    pq.write_table(
        pa.table(
            {
                "stop_id": ["S1", "S2"],
                "stop_name": ["Stazione", "Pontevigodarzere"],
                "stop_lat": ["45.4175", "45.4340"],
                "stop_lon": ["11.8809", "11.8720"],
            }
        ),
        ver_dir / "stops.parquet",
    )

    pq.write_table(
        pa.table({"route_id": ["T1"], "route_short_name": ["SIR1"], "route_type": ["0"]}),
        ver_dir / "routes.parquet",
    )

    pq.write_table(
        pa.table(
            {
                "route_id": ["T1", "T1"],
                "service_id": ["1", "1"],
                "trip_id": ["trip-1", "trip-night"],
                "direction_id": ["0", "1"],
            }
        ),
        ver_dir / "trips.parquet",
    )

    pq.write_table(
        pa.table(
            {
                "trip_id": ["trip-1", "trip-1", "trip-night"],
                "arrival_time": ["08:00:00", "08:10:00", "25:15:00"],
                "departure_time": ["08:01:00", "08:11:00", "25:16:00"],
                "stop_id": ["S1", "S2", "S1"],
                "stop_sequence": ["1", "2", "1"],
            }
        ),
        ver_dir / "stop_times.parquet",
    )

    pq.write_table(
        pa.table(
            {
                "service_id": ["1", "1"],
                "date": ["20260730", "20260731"],
                "exception_type": ["1", "1"],
            }
        ),
        ver_dir / "calendar_dates.parquet",
    )

    # ── RT trip updates ──────────────────────────────────────────────────
    tu_dir = data_dir / "trip_updates" / "date=2026-07-30" / "hour=08"
    tu_dir.mkdir(parents=True)

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "feed_timestamp": 1785427200,
                    "entity_id": "e1",
                    "trip_id": "trip-1",
                    "route_id": "T1",
                    "direction_id": 0,
                    "start_date": "20260730",
                    "start_time": "08:00:00",
                    "vehicle_id": "tram-01",
                    "vehicle_label": "Tram 01",
                    "stop_sequence": 1,
                    "stop_id": "S1",
                    "arrival_delay": 60,
                    "arrival_time": 0,
                    "departure_delay": 75,
                    "departure_time": 0,
                    "schedule_relationship": 0,
                },
                {
                    "feed_timestamp": 1785427200,
                    "entity_id": "e1",
                    "trip_id": "trip-1",
                    "route_id": "T1",
                    "direction_id": 0,
                    "start_date": "20260730",
                    "start_time": "08:00:00",
                    "vehicle_id": "tram-01",
                    "vehicle_label": "Tram 01",
                    "stop_sequence": 2,
                    "stop_id": "S2",
                    "arrival_delay": -30,
                    "arrival_time": 0,
                    "departure_delay": 0,
                    "departure_time": 0,
                    "schedule_relationship": 0,
                },
                {
                    "feed_timestamp": 1785427200,
                    "entity_id": "e2",
                    "trip_id": "trip-night",
                    "route_id": "T1",
                    "direction_id": 1,
                    "start_date": "20260730",
                    "start_time": "25:15:00",
                    "vehicle_id": "tram-02",
                    "vehicle_label": "Tram 02",
                    "stop_sequence": 1,
                    "stop_id": "S1",
                    "arrival_delay": 120,
                    "arrival_time": 0,
                    "departure_delay": 120,
                    "departure_time": 0,
                    "schedule_relationship": 0,
                },
            ],
            schema=TRIP_UPDATES_SCHEMA,
        ),
        tu_dir / "20260730T080000.parquet",
    )

    # ── RT vehicle positions ─────────────────────────────────────────────
    vp_dir = data_dir / "vehicle_positions" / "date=2026-07-30" / "hour=08"
    vp_dir.mkdir(parents=True)

    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "feed_timestamp": 1785427200,
                    "entity_id": "v1",
                    "trip_id": "trip-1",
                    "route_id": "T1",
                    "direction_id": 0,
                    "start_date": "20260730",
                    "start_time": "08:00:00",
                    "vehicle_id": "tram-01",
                    "vehicle_label": "Tram 01",
                    "latitude": 45.4175,
                    "longitude": 11.8809,
                    "bearing": 180.0,
                    "speed": 10.0,
                    "current_stop_sequence": 1,
                    "stop_id": "S1",
                    "current_status": 1,
                    "timestamp": 1785427200,
                }
            ],
            schema=VEHICLE_POSITIONS_SCHEMA,
        ),
        vp_dir / "20260730T080000.parquet",
    )


@pytest.fixture(scope="session")
def dbt_warehouse():
    """Run dbt once against synthetic data, return the DuckDB path.

    Session-scoped so dbt only runs once across all test modules.
    """
    dbt_mod = pytest.importorskip("dbt.cli.main", reason="dbt-duckdb not installed")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _create_synthetic_data(data_dir)

        db_path = tmp_path / "test.duckdb"

        old_env = os.environ.get("DUCKDB_PATH")
        os.environ["DUCKDB_PATH"] = str(db_path)
        try:
            runner = dbt_mod.dbtRunner()
            result = runner.invoke(
                [
                    "run",
                    "--profiles-dir",
                    str(DBT_DIR),
                    "--project-dir",
                    str(DBT_DIR),
                    "--vars",
                    f'{{"data_dir": "{data_dir.as_posix()}"}}',
                ]
            )
            assert result.success, f"dbt run failed: {result.exception or result.result}"

            yield str(db_path)
        finally:
            if old_env is None:
                os.environ.pop("DUCKDB_PATH", None)
            else:
                os.environ["DUCKDB_PATH"] = old_env
