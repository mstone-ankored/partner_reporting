{{ config(materialized='table') }}

-- Partner rankings + flags, computed off the all_time summary row so that
-- ranks reflect cumulative performance. Provides ready-to-use columns for the
-- dashboard's "leaderboard" view without requiring window functions in BI.
--
-- Volume/efficiency quadrant uses the MEDIAN of leads (volume) and lead→won
-- rate (efficiency) as cutpoints; above-median = high, below-median = low.

with base as (
    select
        partner_id,
        partner_name,
        total_leads,
        total_deals_created,
        deals_closed_won,
        revenue_closed_won,
        deal_to_won_rate,
        lead_to_won_rate,
        revenue_per_lead,
        avg_deal_size,
        mql_rate,
        sql_rate,
        partner_penetration_rate
    from {{ ref('partner_summary') }}
    where period_type = 'all_time'
),

cutpoints as (
    select
        {{ approx_median('total_leads') }}       as median_leads,
        {{ approx_median('lead_to_won_rate') }}  as median_lead_to_won_rate
    from base
),

ranked as (
    select
        b.*,
        c.median_leads,
        c.median_lead_to_won_rate,
        rank() over (order by b.revenue_closed_won desc nulls last)  as rank_by_revenue,
        rank() over (order by b.deal_to_won_rate   desc nulls last)  as rank_by_close_rate,
        rank() over (order by b.revenue_per_lead   desc nulls last)  as rank_by_revenue_per_lead,
        rank() over (order by b.total_leads        desc nulls last)  as rank_by_lead_volume,
        case
            when b.total_leads     >= c.median_leads
             and b.lead_to_won_rate >= c.median_lead_to_won_rate then 'high_volume_high_conversion'
            when b.total_leads     >= c.median_leads
             and b.lead_to_won_rate <  c.median_lead_to_won_rate then 'high_volume_low_conversion'
            when b.total_leads     <  c.median_leads
             and b.lead_to_won_rate >= c.median_lead_to_won_rate then 'low_volume_high_conversion'
            else                                                       'low_volume_low_conversion'
        end                                                            as volume_efficiency_quadrant
    from base b
    cross join cutpoints c
)

select
    *,
    case
        when rank_by_revenue <= 5 or volume_efficiency_quadrant = 'high_volume_high_conversion'
            then true else false
    end                                                              as is_top_performer,
    case
        when volume_efficiency_quadrant = 'low_volume_high_conversion' then true else false
    end                                                              as is_high_potential,
    case
        when total_leads >= median_leads
         and lead_to_won_rate < median_lead_to_won_rate / 2
            then true else false
    end                                                              as is_underperformer
from ranked
