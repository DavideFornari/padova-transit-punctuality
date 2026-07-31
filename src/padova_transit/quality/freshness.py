"""Check freshness of ingested data and raise on staleness.

Pure functions — no Airflow imports, fully testable.
"""

from __future__ import annotations

import glob
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class StaleDataError(Exception):
    """Raised when ingested data is older than the allowed threshold."""


ROME = ZoneInfo("Europe/Rome")
# The tram does not run roughly 00:00-05:00 Europe/Rome; a freshness alarm in
# that window would be a permanent nightly false positive.
QUIET_HOURS = range(0, 5)


def latest_rt_feed_timestamp(base_dir: Path, feed_name: str) -> int | None:
    """Return the max feed_timestamp from the newest Parquet files of a RT feed.

    Filenames embed the UTC poll timestamp, so sorting the glob descending puts
    the newest file first — only a handful of files are read, not the history.
    Returns None if no files exist (first deploy, or outside service hours).
    """
    pattern = str(base_dir / feed_name / "date=*" / "hour=*" / "*.parquet")
    files = sorted(glob.glob(pattern), reverse=True)
    for f in files[:5]:
        table = pq.read_table(f, columns=["feed_timestamp"])
        if table.num_rows > 0:
            return max(table.column("feed_timestamp").to_pylist())
    return None


def latest_static_download(base_dir: Path) -> datetime | None:
    """Return the most recent downloaded_at from the versions manifest.

    Returns None if no manifest exists.
    """
    manifest_path = base_dir / "gtfs_static" / "_versions.parquet"
    if not manifest_path.exists():
        return None

    table = pq.read_table(manifest_path, columns=["downloaded_at"])
    if table.num_rows == 0:
        return None

    dates = table.column("downloaded_at").to_pylist()
    parsed = [datetime.fromisoformat(d) for d in dates]
    return max(d if d.tzinfo else d.replace(tzinfo=UTC) for d in parsed)


def check_rt_freshness(
    base_dir: Path,
    feed_name: str,
    now: datetime,
    max_age: timedelta = timedelta(minutes=10),
) -> None:
    """Raise StaleDataError if the latest RT feed is older than *max_age*.

    A missing feed (None) during service hours is also treated as stale.
    Outside typical service hours (roughly 00:00-05:00 Europe/Rome) we
    skip the check since the tram doesn't run. *now* must be timezone-aware.
    """
    if now.astimezone(ROME).hour in QUIET_HOURS:
        logger.info("Quiet hours in Europe/Rome — skipping %s freshness check", feed_name)
        return

    ts = latest_rt_feed_timestamp(base_dir, feed_name)

    if ts is None:
        logger.warning("No RT data found for %s — may be outside service hours", feed_name)
        return

    feed_time = datetime.fromtimestamp(ts, tz=UTC)
    age = now - feed_time

    if age > max_age:
        raise StaleDataError(
            f"{feed_name} feed is stale: last update {feed_time.isoformat()} "
            f"({age} ago, threshold {max_age})"
        )

    logger.info("%s feed is fresh: last update %s (%s ago)", feed_name, feed_time.isoformat(), age)


def check_static_freshness(
    base_dir: Path,
    now: datetime,
    max_age: timedelta = timedelta(days=14),
) -> None:
    """Raise StaleDataError if the static GTFS feed hasn't been updated recently."""
    last_download = latest_static_download(base_dir)

    if last_download is None:
        raise StaleDataError("No static GTFS versions found in manifest")

    age = now - last_download

    if age > max_age:
        raise StaleDataError(
            f"Static GTFS is stale: last download {last_download.isoformat()} "
            f"({age.days} days ago, threshold {max_age.days} days)"
        )

    logger.info(
        "Static GTFS is fresh: last download %s (%s days ago)",
        last_download.isoformat(),
        age.days,
    )
