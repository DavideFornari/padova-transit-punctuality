-- GTFS routes, read from all versions via hive partitioning.

select
    version,
    route_id,
    route_short_name,
    cast(route_type as integer) as route_type
from read_parquet(
    '{{ var("data_dir") }}/gtfs_static/version=*/routes.parquet',
    hive_partitioning = true
)
