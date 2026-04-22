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

        -- The name of the HubSpot contact property carrying the partner
        -- dropdown is configurable via dbt vars so different instances can
        -- map their own custom fields.
        {{ hubspot_property('property', var('partner_name_contact_property')) }}  as referring_partner_raw,
        property_lead_origin                                        as lead_origin_raw,
        property_lead_source                                        as lead_source_raw,
        property_partner_source                                     as partner_source_notes_raw,

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
    nullif(trim(referring_partner_raw), '')                         as referring_partner_declared,
    lower(nullif(trim(lead_origin_raw), ''))                        as lead_origin_declared,
    lower(nullif(trim(lead_source_raw), ''))                        as lead_source_declared,
    nullif(trim(partner_source_notes_raw), '')                      as partner_source_notes,
    _synced_at
from renamed
