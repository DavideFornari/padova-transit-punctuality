"""Mirror the local Parquet landing zone to Google Cloud Storage.

Optional — every function is a no-op unless ``GCS_BUCKET`` is set.  The
ingest functions always write locally first; this module then mirrors the
file, so the local-only path keeps working with no cloud account.

Object keys mirror the local layout exactly::

    <DATA_DIR>/trip_updates/date=2026-07-30/hour=08/20260730T081500.parquet
    gs://<bucket>/trip_updates/date=2026-07-30/hour=08/20260730T081500.parquet

Keeping the Hive partition directories in the key is what lets the cloud
dbt target glob ``gs://<bucket>/trip_updates/date=*/hour=*/*.parquet`` and
still recover ``date`` and ``hour`` as columns.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)


def is_cloud_enabled() -> bool:
    """Return True if cloud mode is configured."""
    return bool(os.getenv("GCS_BUCKET"))


@lru_cache(maxsize=1)
def _client():
    """Return a cached GCS client.

    Imported lazily and cached because building a client resolves
    credentials, which is wasted work in local mode and per-file overhead
    when mirroring a whole directory.
    """
    from google.cloud import storage  # lazy import — not installed in local mode

    return storage.Client()


def blob_name(local_path: Path, data_dir: Path) -> str:
    """Return the GCS object key for a local file, preserving its partitions.

    Raises ValueError if the file is not inside ``data_dir`` — that would
    otherwise silently produce a key outside the mirrored layout.
    """
    relative = local_path.resolve().relative_to(data_dir.resolve())
    return relative.as_posix()


def upload_to_gcs(local_path: Path, data_dir: Path) -> str | None:
    """Upload a single file, mirroring its path relative to ``data_dir``.

    Returns the ``gs://`` URI on success, or None if cloud mode is disabled.
    """
    bucket_name = os.getenv("GCS_BUCKET")
    if not bucket_name:
        return None

    key = blob_name(local_path, data_dir)
    _client().bucket(bucket_name).blob(key).upload_from_filename(str(local_path))

    uri = f"gs://{bucket_name}/{key}"
    logger.info("Uploaded %s to %s", local_path, uri)
    return uri


def mirror_directory_to_gcs(local_dir: Path, data_dir: Path) -> list[str]:
    """Upload every Parquet file under ``local_dir``, preserving the layout.

    Returns the ``gs://`` URIs uploaded (empty when cloud mode is disabled).
    """
    if not is_cloud_enabled():
        return []

    uris = []
    for parquet_file in sorted(local_dir.rglob("*.parquet")):
        uri = upload_to_gcs(parquet_file, data_dir)
        if uri:
            uris.append(uri)

    logger.info("Mirrored %d file(s) from %s to GCS", len(uris), local_dir)
    return uris
