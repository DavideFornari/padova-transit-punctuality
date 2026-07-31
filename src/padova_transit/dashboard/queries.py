"""Dashboard queries against the DuckDB warehouse.

All functions take a DuckDB connection and return lists of dicts, keeping
the Streamlit app free of SQL and making the logic testable without pandas.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def _fetchdicts(result) -> list[dict]:
    """Convert a DuckDB query result to a list of dicts."""
    cols = [desc[0] for desc in result.description]
    return [dict(zip(cols, row, strict=True)) for row in result.fetchall()]


def punctuality_by_route(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Average and median arrival delay per route."""
    return _fetchdicts(
        con.sql("""
            select
                f.route_id,
                r.route_short_name,
                count(*)                                     as total_events,
                round(avg(f.arrival_delay_seconds), 1)       as avg_delay_sec,
                round(median(f.arrival_delay_seconds), 1)    as median_delay_sec,
                round(100.0 * count(*) filter (
                    where f.arrival_delay_seconds between -60 and 300
                ) / count(*), 1)                             as pct_on_time
            from fct_stop_events f
            left join dim_routes r on f.route_id = r.route_id
            where f.arrival_delay_seconds is not null
            group by f.route_id, r.route_short_name
            order by avg_delay_sec desc
        """)
    )


def punctuality_by_hour(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Average arrival delay by hour of day."""
    return _fetchdicts(
        con.sql("""
            select
                extract(hour from scheduled_arrival) as hour_of_day,
                count(*)                                     as total_events,
                round(avg(arrival_delay_seconds), 1)         as avg_delay_sec,
                round(median(arrival_delay_seconds), 1)      as median_delay_sec,
                round(100.0 * count(*) filter (
                    where arrival_delay_seconds between -60 and 300
                ) / count(*), 1)                             as pct_on_time
            from fct_stop_events
            where arrival_delay_seconds is not null
            group by hour_of_day
            order by hour_of_day
        """)
    )


def punctuality_by_stop(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Average arrival delay per stop, with coordinates for map display."""
    return _fetchdicts(
        con.sql("""
            select
                f.stop_id,
                s.stop_name,
                s.stop_lat,
                s.stop_lon,
                count(*)                                     as total_events,
                round(avg(f.arrival_delay_seconds), 1)       as avg_delay_sec,
                round(median(f.arrival_delay_seconds), 1)    as median_delay_sec,
                round(100.0 * count(*) filter (
                    where f.arrival_delay_seconds between -60 and 300
                ) / count(*), 1)                             as pct_on_time
            from fct_stop_events f
            left join dim_stops s on f.stop_id = s.stop_id
            where f.arrival_delay_seconds is not null
            group by f.stop_id, s.stop_name, s.stop_lat, s.stop_lon
            order by avg_delay_sec desc
        """)
    )


def delay_distribution(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Histogram buckets of arrival delay for overall distribution."""
    return _fetchdicts(
        con.sql("""
            select
                case
                    when arrival_delay_seconds < -60  then '< -1 min (early)'
                    when arrival_delay_seconds < 0    then '-1 to 0 min'
                    when arrival_delay_seconds < 60   then '0 to 1 min'
                    when arrival_delay_seconds < 180  then '1 to 3 min'
                    when arrival_delay_seconds < 300  then '3 to 5 min'
                    when arrival_delay_seconds < 600  then '5 to 10 min'
                    else '> 10 min (late)'
                end as delay_bucket,
                case
                    when arrival_delay_seconds < -60  then 1
                    when arrival_delay_seconds < 0    then 2
                    when arrival_delay_seconds < 60   then 3
                    when arrival_delay_seconds < 180  then 4
                    when arrival_delay_seconds < 300  then 5
                    when arrival_delay_seconds < 600  then 6
                    else 7
                end as bucket_order,
                count(*) as event_count
            from fct_stop_events
            where arrival_delay_seconds is not null
            group by delay_bucket, bucket_order
            order by bucket_order
        """)
    )
