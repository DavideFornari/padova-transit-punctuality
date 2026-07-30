"""Feed URLs and shared constants for the Padova transit pipeline."""

# ── GTFS static schedule (zip) ───────────────────────────────────────────────
GTFS_STATIC_BUS_URL = "https://gtfs-biv.fsbusitalia.com/GTFS-BIV/gtfs-biv.zip"
GTFS_STATIC_TRAM_URL = "https://gtfs-biv.fsbusitalia.com/GTFS-BIV-TRAM/gtfs-biv-tram.zip"
GTFS_STATIC_HISTORY_URL = "https://gtfs-biv.fsbusitalia.com/GTFS-BIV-HISTORY/"

# ── GTFS-Realtime (protobuf) ─────────────────────────────────────────────────
GTFSRT_BUS_TRIP_UPDATES_URL = (
    "https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV/start-gtfs-rt-trip-updates-fc.pb"
)
GTFSRT_BUS_VEHICLE_POSITIONS_URL = (
    "https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV/start-gtfs-rt-vehicle-positions-fc.pb"
)
GTFSRT_TRAM_TRIP_UPDATES_URL = (
    "https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-trip-updates.pb"
)
GTFSRT_TRAM_VEHICLE_POSITIONS_URL = (
    "https://gtfs-biv.fsbusitalia.com/GTFSRT-BIV-TRAM/gtfs-rt-vehicle-positions.pb"
)

# ── Timezone ──────────────────────────────────────────────────────────────────
TIMEZONE = "Europe/Rome"
