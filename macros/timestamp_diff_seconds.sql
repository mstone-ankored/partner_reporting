{# Warehouse-agnostic seconds-between-two-timestamps.
   BigQuery: TIMESTAMP_DIFF(end, start, SECOND)
   Postgres / Redshift: EXTRACT(EPOCH FROM end - start)
   Snowflake: TIMESTAMPDIFF(SECOND, start, end) #}

{% macro timestamp_diff_seconds(end_ts, start_ts) %}
    {%- if target.type == 'bigquery' -%}
        timestamp_diff({{ end_ts }}, {{ start_ts }}, second)
    {%- elif target.type == 'snowflake' -%}
        timestampdiff(second, {{ start_ts }}, {{ end_ts }})
    {%- else -%}
        extract(epoch from ({{ end_ts }} - {{ start_ts }}))
    {%- endif -%}
{% endmacro %}
