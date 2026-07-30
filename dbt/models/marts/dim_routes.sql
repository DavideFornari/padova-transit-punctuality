-- Dimension: routes.
-- Takes the latest version of each route.

with ranked as (
    select
        r.route_id,
        r.route_short_name,
        r.route_type,
        v.downloaded_at,
        row_number() over (
            partition by r.route_id
            order by v.downloaded_at desc
        ) as rn
    from {{ ref('stg_routes') }} r
    inner join {{ ref('stg_gtfs_versions') }} v
        on left(v.version, 12) = r.version
)

select
    route_id,
    route_short_name,
    route_type
from ranked
where rn = 1
