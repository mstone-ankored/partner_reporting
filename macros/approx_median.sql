{# Warehouse-agnostic median helper.
   BigQuery: use APPROX_QUANTILES (50th percentile).
   Snowflake: MEDIAN(...) exists natively.
   Redshift / Postgres: PERCENTILE_CONT(0.5) WITHIN GROUP (...).
   We dispatch on the target warehouse so the same model SQL compiles cleanly. #}

{% macro approx_median(expr) %}
    {%- if target.type == 'bigquery' -%}
        approx_quantiles({{ expr }}, 100)[offset(50)]
    {%- elif target.type == 'snowflake' -%}
        median({{ expr }})
    {%- elif target.type in ('postgres', 'redshift') -%}
        percentile_cont(0.5) within group (order by {{ expr }})
    {%- else -%}
        avg({{ expr }}) /* fallback: your warehouse does not have a median fn; approximate with mean */
    {%- endif -%}
{% endmacro %}
