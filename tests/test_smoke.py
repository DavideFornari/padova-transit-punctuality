"""Smoke test — verifies the package is importable and constants are defined."""

from padova_transit.constants import GTFSRT_TRAM_TRIP_UPDATES_URL, TIMEZONE


def test_constants_are_defined() -> None:
    assert TIMEZONE == "Europe/Rome"
    assert GTFSRT_TRAM_TRIP_UPDATES_URL.endswith(".pb")
