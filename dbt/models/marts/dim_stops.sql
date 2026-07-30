-- Dimension: stops.
-- Takes the latest version of each stop (by version download order).

with ranked as (
    select
        s.stop_id,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        v.downloaded_at,
        row_number() over (
            partition by s.stop_id
            order by v.downloaded_at desc
        ) as rn
    from {{ ref('stg_stops') }} s
    inner join {{ ref('stg_gtfs_versions') }} v
        on left(v.version, 12) = s.version
)

select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon
from ranked
where rn = 1
