{# Warehouse-agnostic safe division.
   BigQuery has SAFE_DIVIDE(a, b) natively. Postgres / Snowflake / Redshift do
   not, so we dispatch on target and fall back to `case when` + nullif. #}

{% macro safe_divide(numerator, denominator) %}
    {%- if target.type == 'bigquery' -%}
        safe_divide({{ numerator }}, {{ denominator }})
    {%- else -%}
        (case when nullif({{ denominator }}, 0) is null then null
              else ({{ numerator }})::numeric / nullif({{ denominator }}, 0) end)
    {%- endif -%}
{% endmacro %}
