{# Portable "extract first regex capture group from a string column".
   BigQuery: REGEXP_EXTRACT(s, r'pattern')
   Postgres: substring(s from 'pattern')
   Snowflake: REGEXP_SUBSTR(s, 'pattern', 1, 1, 'e')
   Redshift: similar to Postgres. #}

{% macro regex_extract_group(column_expr, pattern) %}
    {%- if target.type == 'bigquery' -%}
        regexp_extract({{ column_expr }}, r'{{ pattern }}')
    {%- elif target.type == 'snowflake' -%}
        regexp_substr({{ column_expr }}, '{{ pattern }}', 1, 1, 'e')
    {%- else -%}
        substring({{ column_expr }} from '{{ pattern }}')
    {%- endif -%}
{% endmacro %}
