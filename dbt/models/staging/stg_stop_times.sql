-- GTFS stop_times, read from all versions via hive partitioning.
-- Converts GTFS time strings (which may be >= 24:00:00 for trips crossing
-- midnight) into DuckDB INTERVALs from midnight of the service day.

select
    version,
    trip_id,
    cast(stop_sequence as integer) as stop_sequence,
    stop_id,
    arrival_time   as arrival_time_str,
    departure_time as departure_time_str,
    {{ gtfs_time_to_interval('arrival_time') }}   as arrival_interval,
    {{ gtfs_time_to_interval('departure_time') }}  as departure_interval
from read_parquet(
    '{{ var("data_dir") }}/gtfs_static/version=*/stop_times.parquet',
    hive_partitioning = true
)
