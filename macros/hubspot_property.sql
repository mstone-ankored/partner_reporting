{# Resolve a HubSpot property column whose name is configurable via dbt vars.
   Fivetran lands HubSpot custom properties as `property_<propname>`. Call as:

       {{ hubspot_property('property', var('partner_name_contact_property')) }}

   If the var is null, returns NULL-casted-to-string so the model still compiles
   on instances that do not have the custom property configured. #}

{% macro hubspot_property(prefix, property_name) %}
    {%- if property_name is none or property_name == '' -%}
        cast(null as string)
    {%- else -%}
        {{ prefix }}_{{ property_name }}
    {%- endif -%}
{% endmacro %}
