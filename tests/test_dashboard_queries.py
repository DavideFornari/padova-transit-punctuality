"""Tests for dashboard query functions against a real DuckDB warehouse.

Uses the dbt_warehouse fixture from conftest.py.
"""

from __future__ import annotations

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt-duckdb not installed")

from padova_transit.dashboard.queries import (
    delay_distribution,
    punctuality_by_hour,
    punctuality_by_route,
    punctuality_by_stop,
)


def test_punctuality_by_route(dbt_warehouse: str) -> None:
    con = duckdb.connect(dbt_warehouse)
    rows = punctuality_by_route(con)
    assert len(rows) >= 1
    row = rows[0]
    assert "route_id" in row
    assert "avg_delay_sec" in row
    assert "pct_on_time" in row
    assert row["total_events"] > 0


def test_punctuality_by_hour(dbt_warehouse: str) -> None:
    con = duckdb.connect(dbt_warehouse)
    rows = punctuality_by_hour(con)
    assert len(rows) >= 1
    assert "hour_of_day" in rows[0]
    assert "avg_delay_sec" in rows[0]


def test_punctuality_by_stop(dbt_warehouse: str) -> None:
    con = duckdb.connect(dbt_warehouse)
    rows = punctuality_by_stop(con)
    assert len(rows) >= 1
    assert "stop_id" in rows[0]
    assert "stop_name" in rows[0]
    assert "stop_lat" in rows[0]


def test_delay_distribution(dbt_warehouse: str) -> None:
    con = duckdb.connect(dbt_warehouse)
    rows = delay_distribution(con)
    assert len(rows) >= 1
    total = sum(r["event_count"] for r in rows)
    assert total == 3  # matches our synthetic data: 3 stop events
