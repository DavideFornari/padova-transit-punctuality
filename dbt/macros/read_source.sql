{% macro source_root() -%}
{#
    Root the staging models are read from.

    - dev target:   the local Parquet landing zone (var data_dir).
    - cloud target: the same layout mirrored in GCS by the ingestion DAGs.

    Both targets are DuckDB, so the SQL dialect never changes — only the
    location of the files does.  env_var has no default on purpose: selecting
    the cloud target without a bucket should fail loudly, not read nothing.
#}
{%- if target.name == 'cloud' -%}
gs://{{ env_var('GCS_BUCKET') }}
{%- else -%}
{{ var('data_dir') }}
{%- endif -%}
{%- endmacro %}


{% macro read_source(path, hive_partitioning=false) %}
{#
    Read a Parquet source relative to source_root().

    Usage in staging models:
        select * from {{ read_source('gtfs_static/version=*/stops.parquet', hive_partitioning=true) }}
#}
    read_parquet(
        '{{ source_root() }}/{{ path }}'
        {%- if hive_partitioning %}, hive_partitioning = true{% endif %}
    )
{% endmacro %}
