{% macro gtfs_time_to_interval(time_col) %}
{#
    Convert a GTFS time string (e.g. '25:30:00') to a DuckDB INTERVAL.

    GTFS allows hours >= 24 for trips that cross midnight but belong to
    the previous service day.  We parse H:MM:SS manually so that '25:30:00'
    becomes an interval of 25 hours 30 minutes rather than failing with a
    time-parse error.
#}
(  cast(split_part({{ time_col }}, ':', 1) as integer) * interval '1 hour'
 + cast(split_part({{ time_col }}, ':', 2) as integer) * interval '1 minute'
 + cast(split_part({{ time_col }}, ':', 3) as integer) * interval '1 second')
{% endmacro %}
