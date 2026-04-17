{{ config(materialized='view') }}

-- Canonical partner dimension. Joins against the `ref_partners` seed so that
-- partner_name spellings, email domains, and tiers are standardized.

with seeded as (
    select
        partner_id,
        partner_name                                as partner_name,
        lower(trim(partner_name))                   as partner_name_key,
        lower(partner_domain)                       as partner_domain,
        partner_tier,
        partner_start_date
    from {{ ref('ref_partners') }}
)

select * from seeded
