"""Download, version, and persist GTFS static schedule as Parquet files.

Each unique version of the feed (identified by SHA-256 of the zip) is stored
once.  A manifest file tracks version hashes, download timestamps, and the
date range each version covers (derived from calendar_dates.txt).
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import zipfile
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

logger = logging.getLogger(__name__)

# GTFS tables we care about — others (shapes, agency) are ignored for now.
GTFS_TABLES = ("stops", "routes", "trips", "stop_times", "calendar_dates")

VERSIONS_SCHEMA = pa.schema(
    [
        ("version", pa.string()),
        ("downloaded_at", pa.string()),
        ("valid_from", pa.string()),
        ("valid_to", pa.string()),
    ]
)


# ── Fetch & hash ─────────────────────────────────────────────────────────────


def fetch_gtfs_zip(url: str, *, timeout: int = 60) -> bytes:
    """Download a GTFS zip archive and return raw bytes."""
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content


def hash_bytes(data: bytes) -> str:
    """Return the hex SHA-256 digest of *data*."""
    return hashlib.sha256(data).hexdigest()


# ── Manifest ─────────────────────────────────────────────────────────────────


def manifest_path(base_dir: Path) -> Path:
    """Return the path of the versions manifest under ``base_dir``."""
    return base_dir / "gtfs_static" / "_versions.parquet"


def read_manifest(base_dir: Path) -> list[dict]:
    """Read the versions manifest, returning an empty list if it doesn't exist."""
    path = manifest_path(base_dir)
    if not path.exists():
        return []
    table = pq.read_table(path)
    return table.to_pylist()


def _write_manifest(base_dir: Path, rows: list[dict]) -> Path:
    """Write (overwrite) the versions manifest."""
    path = manifest_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=VERSIONS_SCHEMA)
    pq.write_table(table, path)
    return path


def version_exists(base_dir: Path, version_hash: str) -> bool:
    """Check whether a version with this hash has already been ingested."""
    return any(r["version"] == version_hash for r in read_manifest(base_dir))


# ── Extract & convert ────────────────────────────────────────────────────────


def _read_csv_from_zip(zf: zipfile.ZipFile, name: str) -> list[dict]:
    """Read a CSV file from a zip archive into a list of dicts."""
    with zf.open(name) as f:
        text = io.TextIOWrapper(f, encoding="utf-8-sig")
        return list(csv.DictReader(text))


def _validity_window(calendar_dates_rows: list[dict]) -> tuple[str, str]:
    """Derive the validity window (min, max date) from calendar_dates rows."""
    dates = [r["date"] for r in calendar_dates_rows]
    return min(dates), max(dates)


def extract_and_convert(
    zip_bytes: bytes,
    base_dir: Path,
    version_hash: str,
) -> tuple[Path, str, str]:
    """Extract GTFS CSVs from the zip and write them as Parquet files.

    Returns (version_dir, valid_from, valid_to).
    """
    version_dir = base_dir / "gtfs_static" / f"version={version_hash[:12]}"
    version_dir.mkdir(parents=True, exist_ok=True)

    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    available = {Path(n).stem: n for n in zf.namelist()}

    valid_from = ""
    valid_to = ""

    for table_name in GTFS_TABLES:
        filename = f"{table_name}.txt"
        if filename not in available.values():
            logger.warning("GTFS table %s not found in zip, skipping", filename)
            continue

        rows = _read_csv_from_zip(zf, filename)
        if not rows:
            logger.warning("GTFS table %s is empty, skipping", filename)
            continue

        # Derive validity window from calendar_dates
        if table_name == "calendar_dates":
            valid_from, valid_to = _validity_window(rows)

        table = pa.Table.from_pylist(rows)
        out_path = version_dir / f"{table_name}.parquet"
        pq.write_table(table, out_path)
        logger.info("Wrote %s (%d rows)", out_path, len(rows))

    return version_dir, valid_from, valid_to


# ── High-level entry point ───────────────────────────────────────────────────


def ingest_static_gtfs(
    url: str,
    base_dir: Path,
    now: datetime | None = None,
) -> dict:
    """Download the GTFS static feed, version it, and write Parquet files.

    Returns a dict with version info.  Skips extraction if this exact
    version has already been ingested (idempotent).
    """
    if now is None:
        now = datetime.now()

    zip_bytes = fetch_gtfs_zip(url)
    version_hash = hash_bytes(zip_bytes)

    if version_exists(base_dir, version_hash):
        logger.info("Version %s already ingested, skipping", version_hash[:12])
        return {"version": version_hash, "status": "skipped"}

    version_dir, valid_from, valid_to = extract_and_convert(zip_bytes, base_dir, version_hash)

    # Append to manifest
    manifest = read_manifest(base_dir)
    manifest.append(
        {
            "version": version_hash,
            "downloaded_at": now.isoformat(),
            "valid_from": valid_from,
            "valid_to": valid_to,
        }
    )
    _write_manifest(base_dir, manifest)

    logger.info(
        "Ingested GTFS version %s (valid %s to %s) into %s",
        version_hash[:12],
        valid_from,
        valid_to,
        version_dir,
    )
    return {
        "version": version_hash,
        "status": "ingested",
        "version_dir": str(version_dir),
        "valid_from": valid_from,
        "valid_to": valid_to,
    }
