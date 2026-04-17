{{
    config(
        materialized='incremental',
        unique_key='deal_id',
        on_schema_change='append_new_columns',
        incremental_strategy='merge'
    )
}}

-- partner_deals: one row per deal where the deal's primary contact is partner-
-- sourced (per int_partner_contact_attribution), OR the deal itself carries a
-- partner_name on a custom deal-level property.
--
-- Multiple deals per contact are preserved (not collapsed). If you need to
-- collapse, aggregate in downstream reporting layers.

with deals as (
    select * from {{ ref('stg_hubspot__deals') }}
    {% if is_incremental() %}
      where _synced_at >= timestamp_sub(current_timestamp(),
                                        interval {{ var('incremental_lookback_days') }} day)
    {% endif %}
),

primary_contact as (
    select * from {{ ref('int_deal_primary_contact') }}
),

attribution as (
    select * from {{ ref('int_partner_contact_attribution') }}
),

partners as (
    select * from {{ ref('stg_ref__partners') }}
),

owners as (
    select * from {{ ref('stg_hubspot__owners') }}
),

sales_touches as (
    select * from {{ ref('int_deal_sales_touches') }}
),

contacts as (
    select contact_id, contact_created_at from {{ ref('stg_hubspot__contacts') }}
),

-- Combine partner attribution from (1) the contact attribution table and
-- (2) the deal-level custom property, preferring contact-level (it's higher
-- fidelity since it also carries source_type).
deal_partner as (
    select
        d.deal_id,
        coalesce(a.partner_id, pd.partner_id)             as partner_id,
        coalesce(a.partner_name, d.referring_partner_name_deal) as partner_name,
        a.source_type,
        case
            when a.partner_name is not null then 'contact_attribution'
            when d.referring_partner_name_deal is not null then 'deal_property'
            else null
        end                                               as partner_attribution_origin
    from deals d
    left join primary_contact pc using (deal_id)
    left join attribution a
      on a.contact_id = pc.primary_contact_id
    left join partners pd
      on lower(trim(d.referring_partner_name_deal)) = pd.partner_name_key
)

select
    -- Identity
    d.deal_id,
    pc.primary_contact_id                             as contact_id,
    dp.partner_id,
    dp.partner_name,
    dp.source_type,
    dp.partner_attribution_origin,

    -- Deal attributes
    d.deal_name,
    d.pipeline_id,
    d.deal_stage,
    d.deal_stage_probability,
    d.deal_type,

    -- Money
    d.amount,
    d.amount_home_currency,

    -- Dates
    d.deal_created_at,
    date(d.deal_created_at)                           as deal_created_date,
    date_trunc(date(d.deal_created_at), month)        as deal_created_month,
    date_trunc(date(d.deal_created_at), quarter)      as deal_created_quarter,
    d.deal_close_date,
    date(d.deal_close_date)                           as deal_close_date_d,
    date_trunc(date(d.deal_close_date), month)        as deal_close_month,
    date_trunc(date(d.deal_close_date), quarter)      as deal_close_quarter,
    d.deal_closed_won_at,
    c.contact_created_at,

    -- Status flags
    d.is_closed_won,
    d.is_closed,
    d.deal_status,

    -- Durations (days)
    safe_divide(timestamp_diff(d.deal_created_at, c.contact_created_at, second), 86400.0)
                                                      as days_contact_to_deal,
    safe_divide(timestamp_diff(coalesce(d.deal_closed_won_at, d.deal_close_date, current_timestamp()),
                               d.deal_created_at, second), 86400.0)
                                                      as time_to_close_days,

    -- Activity
    st.total_touches                                  as sales_touches_total,
    st.outbound_emails                                as sales_outbound_emails,
    st.calls                                          as sales_calls,
    st.meetings                                       as sales_meetings,

    -- Owner
    d.deal_owner_id,
    o.owner_name                                      as deal_owner_name,
    o.owner_email                                     as deal_owner_email,
    o.owner_team_id                                   as deal_owner_team_id,

    d._synced_at
from deals d
left join primary_contact pc using (deal_id)
left join deal_partner dp using (deal_id)
left join contacts c
  on c.contact_id = pc.primary_contact_id
left join sales_touches st using (deal_id)
left join owners o
  on o.owner_id = d.deal_owner_id
where dp.partner_name is not null
