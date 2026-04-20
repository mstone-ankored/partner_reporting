{# Portable "null typed as a string column" — BigQuery uses STRING, Postgres / Snowflake / Redshift use TEXT/VARCHAR. #}

{% macro null_string() %}
    {%- if target.type == 'bigquery' -%}
        cast(null as string)
    {%- else -%}
        cast(null as text)
    {%- endif -%}
{% endmacro %}
