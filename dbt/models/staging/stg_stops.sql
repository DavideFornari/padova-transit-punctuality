-- GTFS stops, read from all versions via hive partitioning.
-- The `version` column is extracted from the directory path.

select
    version,
    stop_id,
    stop_name,
    cast(stop_lat as double) as stop_lat,
    cast(stop_lon as double) as stop_lon
from {{ read_source('gtfs_static/version=*/stops.parquet', hive_partitioning=true) }}
