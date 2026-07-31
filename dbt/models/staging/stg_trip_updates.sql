-- GTFS-RT trip updates flattened from protobuf.
-- One row per stop_time_update per feed poll.

select
    feed_timestamp,
    to_timestamp(feed_timestamp) as feed_ts,
    entity_id,
    trip_id,
    route_id,
    direction_id,
    start_date,
    strptime(nullif(start_date, ''), '%Y%m%d')::date as service_date,
    start_time,
    vehicle_id,
    vehicle_label,
    stop_sequence,
    stop_id,
    schedule_relationship,
    arrival_delay,
    arrival_time   as arrival_epoch,
    departure_delay,
    departure_time as departure_epoch
from {{ read_source('trip_updates/date=*/hour=*/*.parquet', hive_partitioning=true) }}
-- SCHEDULED (0) only: SKIPPED (1) and NO_DATA (2) updates carry no usable
-- delay and must not become punctuality events.  The raw Parquet keeps them.
where schedule_relationship = 0
