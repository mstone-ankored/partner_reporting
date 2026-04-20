{# Portable "extract a scalar string from a JSON column by key".
   BigQuery:   JSON_VALUE(col, '$.key')
   Postgres:   col::jsonb ->> 'key'    (single top-level key only)
   Snowflake:  col:key::string
   For deeply nested paths, pass the full path as the `key` arg ONLY on BigQuery
   or Snowflake; on Postgres the single-level form is used. #}

{% macro json_value(column_expr, key) %}
    {%- if target.type == 'bigquery' -%}
        json_value({{ column_expr }}, '$.{{ key }}')
    {%- elif target.type == 'snowflake' -%}
        {{ column_expr }}:{{ key }}::string
    {%- else -%}
        {{ column_expr }}::jsonb ->> '{{ key }}'
    {%- endif -%}
{% endmacro %}
