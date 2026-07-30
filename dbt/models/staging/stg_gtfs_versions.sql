-- Manifest of GTFS static schedule versions.
-- Each row represents one unique version of the downloaded zip.

select
    version,
    downloaded_at,
    valid_from,
    valid_to
from {{ read_source('gtfs_static/_versions.parquet') }}
