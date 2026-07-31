"""Integration test: validate dbt models against synthetic Parquet data.

The dbt_warehouse fixture (in conftest.py) creates synthetic Parquet files,
runs dbt, and yields a DuckDB path. Tests query the resulting warehouse.

Requires dbt-duckdb to be installed.  Skipped in CI if unavailable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("dbt.cli.main", reason="dbt-duckdb not installed")


# ── Tests ────────────────────────────────────────────────────────────────────


def test_dbt_run_succeeds(dbt_warehouse: str) -> None:
    assert Path(dbt_warehouse).exists()


def test_fct_stop_events_row_count(dbt_warehouse: str) -> None:
    import duckdb

    con = duckdb.connect(dbt_warehouse)
    count = con.sql("select count(*) from fct_stop_events").fetchone()[0]
    assert count == 3  # 2 stops for trip-1 + 1 for trip-night


def test_fct_stop_events_delay_values(dbt_warehouse: str) -> None:
    import duckdb

    con = duckdb.connect(dbt_warehouse)
    row = con.sql("""
        select arrival_delay_seconds, departure_delay_seconds
        from fct_stop_events
        where trip_id = 'trip-1' and stop_sequence = 1
    """).fetchone()
    assert row == (60, 75)


def test_fct_midnight_crossing(dbt_warehouse: str) -> None:
    """A trip with GTFS time 25:15:00 on service_date 2026-07-30 should
    produce a scheduled_arrival on 2026-07-31."""
    import duckdb

    con = duckdb.connect(dbt_warehouse)
    row = con.sql("""
        select
            scheduled_arrival::varchar as sa,
            arrival_delay_seconds
        from fct_stop_events
        where trip_id = 'trip-night'
    """).fetchone()
    assert "2026-07-31" in row[0], f"Expected next-day date, got {row[0]}"
    assert row[1] == 120


def test_scheduled_hour_is_wall_clock(dbt_warehouse: str) -> None:
    """08:00 GTFS time must land in hour bucket 8 regardless of machine timezone."""
    import duckdb

    con = duckdb.connect(dbt_warehouse)
    hour = con.sql("""
        select extract(hour from scheduled_arrival)
        from fct_stop_events
        where trip_id = 'trip-1' and stop_sequence = 1
    """).fetchone()[0]
    assert hour == 8


def test_overlapping_versions_pick_newest_schedule(dbt_warehouse: str) -> None:
    """Two versions cover 2026-07-30; the newer download must win the join."""
    import duckdb

    con = duckdb.connect(dbt_warehouse)
    version, minute = con.sql("""
        select gtfs_version, extract(minute from scheduled_arrival)
        from fct_stop_events
        where trip_id = 'trip-1' and stop_sequence = 1
    """).fetchone()
    assert version == "ffeeddccbba9"
    assert minute == 5


def test_dim_stops(dbt_warehouse: str) -> None:
    import duckdb

    con = duckdb.connect(dbt_warehouse)
    count = con.sql("select count(*) from dim_stops").fetchone()[0]
    assert count == 2


def test_dim_routes(dbt_warehouse: str) -> None:
    import duckdb

    con = duckdb.connect(dbt_warehouse)
    count = con.sql("select count(*) from dim_routes").fetchone()[0]
    assert count == 1
