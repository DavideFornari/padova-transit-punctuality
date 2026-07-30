"""Tests for data freshness checks."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from padova_transit.quality.freshness import (
    StaleDataError,
    check_rt_freshness,
    check_static_freshness,
    latest_rt_feed_timestamp,
    latest_static_download,
)

# ── Helpers ──────────────────────────────────────────────────────────────────


def _write_rt_parquet(base_dir: Path, feed_name: str, feed_timestamp: int) -> None:
    """Write a minimal RT Parquet file with a given feed_timestamp."""
    out_dir = base_dir / feed_name / "date=2026-07-30" / "hour=08"
    out_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({"feed_timestamp": [feed_timestamp]})
    pq.write_table(table, out_dir / "test.parquet")


def _write_manifest(base_dir: Path, downloaded_at: str) -> None:
    """Write a minimal versions manifest."""
    manifest_dir = base_dir / "gtfs_static"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "version": ["abc123"],
            "downloaded_at": [downloaded_at],
            "valid_from": ["20260728"],
            "valid_to": ["20260803"],
        }
    )
    pq.write_table(table, manifest_dir / "_versions.parquet")


# ── RT freshness ─────────────────────────────────────────────────────────────


def test_latest_rt_feed_timestamp_no_files(tmp_path: Path) -> None:
    assert latest_rt_feed_timestamp(tmp_path, "trip_updates") is None


def test_latest_rt_feed_timestamp(tmp_path: Path) -> None:
    _write_rt_parquet(tmp_path, "trip_updates", 1785427200)
    assert latest_rt_feed_timestamp(tmp_path, "trip_updates") == 1785427200


def test_check_rt_freshness_fresh(tmp_path: Path) -> None:
    now = datetime(2026, 7, 30, 8, 5, 0)
    ts = int(now.timestamp()) - 120  # 2 minutes ago
    _write_rt_parquet(tmp_path, "trip_updates", ts)
    # Should not raise
    check_rt_freshness(tmp_path, "trip_updates", now=now, max_age=timedelta(minutes=10))


def test_check_rt_freshness_stale(tmp_path: Path) -> None:
    now = datetime(2026, 7, 30, 8, 30, 0)
    ts = int(now.timestamp()) - 900  # 15 minutes ago
    _write_rt_parquet(tmp_path, "trip_updates", ts)
    with pytest.raises(StaleDataError, match="stale"):
        check_rt_freshness(tmp_path, "trip_updates", now=now, max_age=timedelta(minutes=10))


def test_check_rt_freshness_no_data_does_not_raise(tmp_path: Path) -> None:
    """No data is not an error — may be outside service hours."""
    now = datetime(2026, 7, 30, 3, 0, 0)
    check_rt_freshness(tmp_path, "trip_updates", now=now)


# ── Static freshness ────────────────────────────────────────────────────────


def test_latest_static_download_no_manifest(tmp_path: Path) -> None:
    assert latest_static_download(tmp_path) is None


def test_latest_static_download(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "2026-07-28T04:00:00")
    result = latest_static_download(tmp_path)
    assert result == datetime(2026, 7, 28, 4, 0, 0)


def test_check_static_freshness_fresh(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "2026-07-28T04:00:00")
    now = datetime(2026, 7, 30, 8, 0, 0)
    check_static_freshness(tmp_path, now=now, max_age=timedelta(days=14))


def test_check_static_freshness_stale(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "2026-07-01T04:00:00")
    now = datetime(2026, 7, 30, 8, 0, 0)
    with pytest.raises(StaleDataError, match="stale"):
        check_static_freshness(tmp_path, now=now, max_age=timedelta(days=14))


def test_check_static_freshness_no_manifest(tmp_path: Path) -> None:
    now = datetime(2026, 7, 30, 8, 0, 0)
    with pytest.raises(StaleDataError, match="No static GTFS versions"):
        check_static_freshness(tmp_path, now=now)
