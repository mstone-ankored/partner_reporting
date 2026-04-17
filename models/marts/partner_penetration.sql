{{ config(materialized='table') }}

-- Partner penetration = (our customers at partner) / (total customers at partner).
-- "our customers at partner" is computed from partner_deals where is_closed_won,
-- de-duplicated by contact's company. "total customers at partner" comes from
-- the seed partner_total_customers, which is refreshed dynamically by dropping
-- in a new CSV row (or by replacing the seed via an external pipeline).
--
-- One row per (partner, as_of_date). New as_of_dates in the seed automatically
-- flow through on the next dbt run.

with totals as (
    select * from {{ ref('stg_ref__partner_total_customers') }}
),

partners as (
    select * from {{ ref('stg_ref__partners') }}
),

our_won_customers as (
    select
        partner_id,
        partner_name,
        -- A "customer" is one unique company that reached closed_won. If
        -- company_name is null we fall back to contact_id.
        count(distinct coalesce(nullif(lower(trim(pd.deal_name)), ''), cast(pd.contact_id as string))) as our_customer_count,
        min(pd.deal_closed_won_at) as our_first_won_at
    from {{ ref('partner_deals') }} pd
    where is_closed_won = true
    group by 1, 2
)

select
    t.partner_id,
    t.partner_name,
    t.as_of_date,
    t.total_customer_count,
    coalesce(oc.our_customer_count, 0)                                   as our_customer_count,
    safe_divide(coalesce(oc.our_customer_count, 0), nullif(t.total_customer_count, 0)) as penetration_rate,
    oc.our_first_won_at
from totals t
left join our_won_customers oc
  on oc.partner_id = t.partner_id
