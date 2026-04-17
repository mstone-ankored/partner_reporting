{{ config(materialized='table') }}

-- One row per (partner_deal, stage). Exposed so the dashboard can:
--   * Plot average days-in-stage per partner
--   * Identify drop-off stages (stages a partner's deals enter but rarely exit
--     forward) — see partner_funnel_stage_conversion for the aggregate view.

select
    pd.partner_id,
    pd.partner_name,
    pd.deal_id,
    sd.deal_stage,
    sd.times_entered,
    sd.total_seconds_in_stage,
    sd.total_days_in_stage,
    sd.first_entered_at,
    sd.last_exited_at,
    pd.deal_status,
    pd.is_closed_won,
    pd.amount
from {{ ref('partner_deals') }} pd
join {{ ref('int_deal_stage_durations') }} sd using (deal_id)
