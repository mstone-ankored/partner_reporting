{# Warehouse-agnostic safe cast.
   BigQuery has SAFE_CAST(x AS T) natively. Postgres approximates it with a
   nullif + direct cast. For numeric/timestamp on Postgres, malformed strings
   will still raise; the source is assumed to be clean text landed by Fivetran/
   Airbyte. Use dbt tests on key columns to catch regressions. #}

{% macro safe_cast(value, bq_type) %}
    {%- if target.type == 'bigquery' -%}
        safe_cast({{ value }} as {{ bq_type }})
    {%- else -%}
        {%- set t = bq_type | lower -%}
        {%- if t in ('int64', 'integer', 'int') -%}
            {%- set pg_type = 'bigint' -%}
        {%- elif t in ('bool', 'boolean') -%}
            {%- set pg_type = 'boolean' -%}
        {%- elif t in ('string', 'text', 'varchar') -%}
            {%- set pg_type = 'text' -%}
        {%- elif t in ('float64', 'float') -%}
            {%- set pg_type = 'double precision' -%}
        {%- elif t in ('numeric', 'decimal', 'bignumeric') -%}
            {%- set pg_type = 'numeric' -%}
        {%- elif t in ('date',) -%}
            {%- set pg_type = 'date' -%}
        {%- elif t in ('timestamp', 'datetime', 'timestamptz') -%}
            {%- set pg_type = 'timestamp' -%}
        {%- else -%}
            {%- set pg_type = bq_type -%}
        {%- endif -%}
        nullif(cast({{ value }} as text), '')::{{ pg_type }}
    {%- endif -%}
{% endmacro %}
