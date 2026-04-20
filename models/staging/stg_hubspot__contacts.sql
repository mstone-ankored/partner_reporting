{{ config(materialized='view') }}

-- One row per HubSpot contact, with columns renamed and lightly typed.
-- All partner-attribution logic happens downstream in int_partner_contact_attribution.
-- We intentionally keep this model free of filtering so it can be reused
-- for non-partner reporting.

with source as (
    select * from {{ source('hubspot', 'contact') }}
),

renamed as (
    select
        id                                                         as contact_id,
        lower(property_email)                                      as email,
        {{ regex_extract_group('lower(property_email)', '@([^@]+)$') }} as email_domain,
        property_firstname                                         as first_name,
        property_lastname                                          as last_name,
        property_company                                           as company_name,
        property_jobtitle                                          as job_title,
        property_industry                                          as industry,
        {{ safe_cast('property_numberofemployees', 'int64') }}     as company_size_employees,
        {{ safe_cast('property_annualrevenue', 'numeric') }}       as company_annual_revenue,
        lower(property_lifecyclestage)                             as lifecycle_stage,
        lower(property_hs_lead_status)                             as lead_status,
        {{ safe_cast('property_createdate', 'timestamp') }}        as contact_created_at,
        {{ safe_cast('property_hs_lifecyclestage_lead_date', 'timestamp') }}       as became_lead_at,
        {{ safe_cast('property_hs_lifecyclestage_marketingqualifiedlead_date', 'timestamp') }} as became_mql_at,
        {{ safe_cast('property_hs_lifecyclestage_salesqualifiedlead_date', 'timestamp') }}     as became_sql_at,
        {{ safe_cast('property_hs_lifecyclestage_opportunity_date', 'timestamp') }}            as became_opportunity_at,
        {{ safe_cast('property_hs_lifecyclestage_customer_date', 'timestamp') }}               as became_customer_at,
        property_hs_analytics_source                                as original_source,
        property_hs_analytics_source_data_1                         as original_source_drill_down_1,
        property_hs_analytics_source_data_2                         as original_source_drill_down_2,
        property_hs_latest_source                                   as latest_source,
        property_hs_latest_source_data_1                            as latest_source_drill_down_1,
        property_hs_latest_source_data_2                            as latest_source_drill_down_2,
        property_hubspot_owner_id                                   as contact_owner_id,

        -- The names of the custom HubSpot properties are configurable via
        -- dbt vars so different HubSpot instances can map their own fields.
        {{ hubspot_property('property', var('partner_name_contact_property')) }}  as referring_partner_name_raw,
        {{ hubspot_property('property', var('partner_source_type_property')) }}   as partner_source_type_raw,

        _fivetran_synced                                            as _synced_at
    from source
)

select
    contact_id,
    email,
    email_domain,
    first_name,
    last_name,
    company_name,
    job_title,
    industry,
    company_size_employees,
    company_annual_revenue,
    lifecycle_stage,
    lead_status,
    contact_created_at,
    became_lead_at,
    became_mql_at,
    became_sql_at,
    became_opportunity_at,
    became_customer_at,
    original_source,
    original_source_drill_down_1,
    original_source_drill_down_2,
    latest_source,
    latest_source_drill_down_1,
    latest_source_drill_down_2,
    contact_owner_id,
    nullif(trim(referring_partner_name_raw), '')                    as referring_partner_name_declared,
    lower(nullif(trim(partner_source_type_raw), ''))                as partner_source_type_declared,
    _synced_at
from renamed
