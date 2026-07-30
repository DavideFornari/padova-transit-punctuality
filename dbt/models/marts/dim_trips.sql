-- Dimension: trips with their route and service info.
-- Version-aware — keeps all version/trip combinations for correct joins.

select
    t.version,
    t.trip_id,
    t.route_id,
    t.service_id,
    t.direction_id,
    r.route_short_name
from {{ ref('stg_trips') }} t
inner join {{ ref('stg_routes') }} r
    on t.route_id = r.route_id
    and t.version = r.version
