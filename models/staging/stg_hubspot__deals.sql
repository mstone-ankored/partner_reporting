{{ config(materialized='view') }}

-- One row per HubSpot deal.
-- partner_name is captured if a custom deal-level property exists; otherwise
-- we inherit partner attribution from the deal's primary contact downstream.

with source as (
    select * from {{ source('hubspot', 'deal') }}
),

renamed as (
    select
        deal_id                                                     as deal_id,
        property_dealname                                           as deal_name,
        property_pipeline                                           as pipeline_id,
        lower(property_dealstage)                                   as deal_stage,
        safe_cast(property_hs_deal_stage_probability as numeric)    as deal_stage_probability,
        safe_cast(property_amount as numeric)                       as amount,
        safe_cast(property_amount_in_home_currency as numeric)      as amount_home_currency,
        safe_cast(property_createdate as timestamp)                 as deal_created_at,
        safe_cast(property_closedate as timestamp)                  as deal_close_date,
        safe_cast(property_hs_closed_won_date as timestamp)         as deal_closed_won_at,
        coalesce(safe_cast(property_hs_is_closed_won as bool), false)  as is_closed_won,
        coalesce(safe_cast(property_hs_is_closed as bool),
                 case when lower(property_dealstage) in ({{ "'" ~ var('closed_won_stages') | join("','") ~ "'" }},
                                                         {{ "'" ~ var('closed_lost_stages') | join("','") ~ "'" }})
                      then true else false end)                    as is_closed,
        property_dealtype                                           as deal_type,
        property_hubspot_owner_id                                   as deal_owner_id,
        {{ hubspot_property('property', var('partner_name_contact_property')) }} as referring_partner_name_deal_raw,
        _fivetran_synced                                            as _synced_at
    from source
)

select
    deal_id,
    deal_name,
    pipeline_id,
    deal_stage,
    deal_stage_probability,
    amount,
    amount_home_currency,
    deal_created_at,
    deal_close_date,
    deal_closed_won_at,
    is_closed_won,
    is_closed,
    case
        when is_closed_won then 'won'
        when is_closed and not is_closed_won then 'lost'
        else 'open'
    end                                                             as deal_status,
    deal_type,
    deal_owner_id,
    nullif(trim(referring_partner_name_deal_raw), '')               as referring_partner_name_deal,
    _synced_at
from renamed
