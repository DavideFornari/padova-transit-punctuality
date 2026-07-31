"""Streamlit dashboard for Padova tram punctuality.

Run with: streamlit run src/padova_transit/dashboard/app.py
"""

from __future__ import annotations

import os

import duckdb
import pandas as pd
import streamlit as st

from padova_transit.dashboard.queries import (
    delay_distribution,
    punctuality_by_hour,
    punctuality_by_route,
    punctuality_by_stop,
)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")


def get_connection():
    """A fresh read-only connection per rerun.

    Deliberately not cached: a cached connection would hold a read lock on the
    DuckDB file for the app's lifetime, blocking `dbt run` from replacing the
    warehouse.  Connecting costs milliseconds per rerun.
    """
    return duckdb.connect(DUCKDB_PATH, read_only=True)


def main():
    st.set_page_config(
        page_title="Padova Tram Punctuality",
        page_icon=":tram:",
        layout="wide",
    )

    st.title("Padova Tram Punctuality")
    st.caption("Real-time vs scheduled performance from GTFS and GTFS-RT feeds")

    con = get_connection()

    # Check if we have data
    try:
        count = con.sql("select count(*) from fct_stop_events").fetchone()[0]
    except duckdb.CatalogException:
        st.warning("No data yet. Run the ingestion DAGs and dbt models first.")
        return

    if count == 0:
        st.info("The fact table is empty. Waiting for data from the ingestion pipeline.")
        return

    st.metric("Total stop events", f"{count:,}")

    # ── Tabs ─────────────────────────────────────────────────────────────
    tab_route, tab_time, tab_stop, tab_dist = st.tabs(
        ["By Route", "By Time of Day", "By Stop", "Delay Distribution"]
    )

    # ── By Route ─────────────────────────────────────────────────────────
    with tab_route:
        st.subheader("Punctuality by route")
        route_data = punctuality_by_route(con)
        if route_data:
            df = pd.DataFrame(route_data)
            st.dataframe(
                df.rename(
                    columns={
                        "route_id": "Route ID",
                        "route_short_name": "Route",
                        "total_events": "Events",
                        "avg_delay_sec": "Avg delay (s)",
                        "median_delay_sec": "Median delay (s)",
                        "pct_on_time": "On-time %",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
            st.bar_chart(df, x="route_short_name", y="avg_delay_sec")
        else:
            st.info("No route data available.")

    # ── By Time of Day ───────────────────────────────────────────────────
    with tab_time:
        st.subheader("Punctuality by hour of day")
        hour_data = punctuality_by_hour(con)
        if hour_data:
            df = pd.DataFrame(hour_data)
            col1, col2 = st.columns(2)
            with col1:
                st.line_chart(df, x="hour_of_day", y="avg_delay_sec")
            with col2:
                st.line_chart(df, x="hour_of_day", y="pct_on_time")
            st.dataframe(
                df.rename(
                    columns={
                        "hour_of_day": "Hour",
                        "total_events": "Events",
                        "avg_delay_sec": "Avg delay (s)",
                        "median_delay_sec": "Median delay (s)",
                        "pct_on_time": "On-time %",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No hourly data available.")

    # ── By Stop ──────────────────────────────────────────────────────────
    with tab_stop:
        st.subheader("Punctuality by stop")
        stop_data = punctuality_by_stop(con)
        if stop_data:
            df = pd.DataFrame(stop_data)

            # Map if coordinates are available
            map_df = df.dropna(subset=["stop_lat", "stop_lon"])
            if not map_df.empty:
                st.map(map_df.rename(columns={"stop_lat": "latitude", "stop_lon": "longitude"}))

            st.dataframe(
                df.rename(
                    columns={
                        "stop_id": "Stop ID",
                        "stop_name": "Stop",
                        "total_events": "Events",
                        "avg_delay_sec": "Avg delay (s)",
                        "median_delay_sec": "Median delay (s)",
                        "pct_on_time": "On-time %",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No stop data available.")

    # ── Delay Distribution ───────────────────────────────────────────────
    with tab_dist:
        st.subheader("Delay distribution")
        dist_data = delay_distribution(con)
        if dist_data:
            df = pd.DataFrame(dist_data)
            st.bar_chart(df, x="delay_bucket", y="event_count")
        else:
            st.info("No distribution data available.")


if __name__ == "__main__":
    main()
