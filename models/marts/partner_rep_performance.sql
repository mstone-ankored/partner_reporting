{{ config(materialized='table') }}

-- Sales-rep performance on partner-sourced deals.
-- One row per (partner, rep). For the "any partner" view, query as:
--
--     select deal_owner_id, deal_owner_name,
--            sum(deals_total), sum(deals_won), ...
--     from partner_rep_performance
--     group by 1, 2;

with deals as (
    select * from {{ ref('partner_deals') }}
)

select
    partner_id,
    max(partner_name)                                                as partner_name,
    deal_owner_id,
    max(deal_owner_name)                                             as deal_owner_name,
    max(deal_owner_email)                                            as deal_owner_email,
    count(distinct deal_id)                                          as deals_total,
    count(distinct case when is_closed_won then deal_id end)         as deals_won,
    count(distinct case when is_closed and not is_closed_won then deal_id end) as deals_lost,
    count(distinct case when not is_closed then deal_id end)         as deals_open,
    {{ safe_divide(
        'count(distinct case when is_closed_won then deal_id end)',
        'count(distinct deal_id)'
    ) }}                                                             as win_rate,
    sum(case when is_closed_won then amount else 0 end)              as revenue_closed_won,
    avg(case when is_closed_won then amount end)                     as avg_deal_size,
    avg(case when is_closed_won then time_to_close_days end)         as avg_cycle_days,
    avg(case when is_closed_won then sales_touches_total end)        as avg_sales_touches
from deals
group by partner_id, deal_owner_id
