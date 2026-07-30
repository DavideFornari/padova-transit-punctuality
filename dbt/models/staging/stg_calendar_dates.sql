-- GTFS calendar_dates, read from all versions via hive partitioning.
-- Parses the YYYYMMDD date string into a proper DATE type.

select
    version,
    service_id,
    strptime(date, '%Y%m%d')::date as service_date,
    cast(exception_type as integer) as exception_type
from read_parquet(
    '{{ var("data_dir") }}/gtfs_static/version=*/calendar_dates.parquet',
    hive_partitioning = true
)
