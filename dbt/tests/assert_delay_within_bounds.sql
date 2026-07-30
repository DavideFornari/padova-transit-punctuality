-- Fail if any stop event has an arrival delay outside [-10 min, +60 min].
-- This is a warning-level canary: extreme values likely indicate a data
-- quality issue (corrupt feed, wrong version join, etc.), not a real
-- tram being an hour late.

select
    service_date,
    trip_id,
    stop_id,
    arrival_delay_seconds
from {{ ref('fct_stop_events') }}
where arrival_delay_seconds < -600
   or arrival_delay_seconds > 3600
