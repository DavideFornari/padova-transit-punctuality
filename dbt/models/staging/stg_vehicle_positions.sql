-- GTFS-RT vehicle positions flattened from protobuf.
-- One row per vehicle per feed poll.

select
    feed_timestamp,
    to_timestamp(feed_timestamp) as feed_ts,
    entity_id,
    trip_id,
    route_id,
    direction_id,
    start_date,
    strptime(start_date, '%Y%m%d')::date as service_date,
    start_time,
    vehicle_id,
    vehicle_label,
    latitude,
    longitude,
    bearing,
    speed,
    current_stop_sequence,
    stop_id,
    current_status,
    "timestamp" as position_epoch,
    to_timestamp("timestamp") as position_ts
from {{ read_source('vehicle_positions/date=*/hour=*/*.parquet', hive_partitioning=true) }}
