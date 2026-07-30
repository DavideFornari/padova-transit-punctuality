-- GTFS trips, read from all versions via hive partitioning.

select
    version,
    route_id,
    service_id,
    trip_id,
    cast(direction_id as integer) as direction_id
from read_parquet(
    '{{ var("data_dir") }}/gtfs_static/version=*/trips.parquet',
    hive_partitioning = true
)
