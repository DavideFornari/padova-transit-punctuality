"""Feed URLs and shared constants for the Padova transit pipeline."""

# ── Primary: Tram (publicly accessible, no auth) ────────────────────────────
GTFS_STATIC_TRAM_URL = "https://gtfs-biv.fsbusitalia.com/GTFS-BIV-TRAM/gtfs-biv-tram.zip"
GTFS_STATIC_TRAM_HISTORY_URL = "https://gtfs-biv.fsbusitalia.com/GTFS-BIV-TRAM-HISTORY/"
GTFSRT_TRAM_TRIP_UPDATES_URL = (
    "https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-trip-updates.pb"
)
GTFSRT_TRAM_VEHICLE_POSITIONS_URL = (
    "https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-vehicle-positions.pb"
)

# ── Secondary: Bus (requires HTTP Basic Auth, future extension) ──────────────
GTFS_STATIC_BUS_URL = "https://gtfs-biv.fsbusitalia.com/GTFS-BIV/gtfs-biv.zip"
GTFS_STATIC_BUS_HISTORY_URL = "https://gtfs-biv.fsbusitalia.com/GTFS-BIV-HISTORY/"
GTFSRT_BUS_TRIP_UPDATES_URL = (
    "https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV/start-gtfs-rt-trip-updates-fc.pb"
)
GTFSRT_BUS_VEHICLE_POSITIONS_URL = (
    "https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV/start-gtfs-rt-vehicle-positions-fc.pb"
)

# ── Timezone ─────────────────────────────────────────────────────────────────
TIMEZONE = "Europe/Rome"
