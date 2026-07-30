-- Fact table: stop-level punctuality events.
--
-- Joins each GTFS-RT trip update (per stop) to the static schedule that was
-- valid on that service date, producing scheduled vs actual timestamps and
-- the resulting delay in seconds.
--
-- Key design decisions:
--   1. Version matching: RT start_date is matched to the GTFS static version
--      whose validity window (from calendar_dates.txt) covers that date.
--   2. Midnight-crossing: GTFS allows times >= 24:00:00.  We add the
--      stop_times interval to midnight of the service_date, so '25:30:00'
--      on service_date 2026-07-30 becomes 2026-07-31 01:30:00.
--   3. Timezone: scheduled times are computed in UTC, then cast to
--      Europe/Rome for display.  RT delays are in seconds (timezone-agnostic).
--   4. Dedup: multiple RT polls may report the same trip+stop.  We keep the
--      latest observation per (service_date, trip_id, stop_sequence).

with version_match as (
    -- Map each RT service_date to the correct GTFS static version.
    select
        tu.service_date,
        tu.trip_id,
        tu.route_id,
        tu.direction_id,
        tu.stop_sequence,
        tu.stop_id,
        tu.vehicle_id,
        tu.vehicle_label,
        tu.arrival_delay,
        tu.departure_delay,
        tu.feed_timestamp,
        left(v.version, 12) as gtfs_version,
        row_number() over (
            partition by tu.service_date, tu.trip_id, tu.stop_sequence
            order by tu.feed_timestamp desc
        ) as rn
    from {{ ref('stg_trip_updates') }} tu
    inner join {{ ref('stg_gtfs_versions') }} v
        on tu.service_date >= strptime(v.valid_from, '%Y%m%d')::date
        and tu.service_date <= strptime(v.valid_to, '%Y%m%d')::date
),

deduped as (
    select * from version_match where rn = 1
),

with_schedule as (
    select
        d.service_date,
        d.trip_id,
        d.route_id,
        d.direction_id,
        d.stop_sequence,
        d.stop_id,
        d.vehicle_id,
        d.vehicle_label,
        d.gtfs_version,

        -- Scheduled timestamps: service_date midnight + GTFS interval,
        -- then timezone-aware in Europe/Rome.
        timezone('Europe/Rome',
            (d.service_date::timestamp + st.arrival_interval)::timestamptz
        ) as scheduled_arrival,
        timezone('Europe/Rome',
            (d.service_date::timestamp + st.departure_interval)::timestamptz
        ) as scheduled_departure,

        -- Delays from the RT feed (seconds; positive = late, negative = early).
        d.arrival_delay   as arrival_delay_seconds,
        d.departure_delay as departure_delay_seconds,

        -- Actual timestamps: scheduled + delay.
        timezone('Europe/Rome',
            (d.service_date::timestamp + st.arrival_interval
             + d.arrival_delay * interval '1 second')::timestamptz
        ) as actual_arrival,
        timezone('Europe/Rome',
            (d.service_date::timestamp + st.departure_interval
             + d.departure_delay * interval '1 second')::timestamptz
        ) as actual_departure

    from deduped d
    inner join {{ ref('stg_stop_times') }} st
        on d.trip_id = st.trip_id
        and d.stop_sequence = st.stop_sequence
        and d.gtfs_version = st.version
)

select * from with_schedule
