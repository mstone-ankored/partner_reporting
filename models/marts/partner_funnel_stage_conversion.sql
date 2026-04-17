{{ config(materialized='table') }}

-- Per-partner, per-stage funnel conversion.
--   deals_entered      = distinct deals that ever entered the stage
--   deals_advanced     = distinct deals that entered a LATER stage in the
--                        configured stage order (i.e. moved forward)
--   deals_closed_won   = subset of deals_entered that ended closed_won
--   stage_conversion_rate = deals_advanced / deals_entered
--
-- The `next_stage_conversion_rate` is the conversion from this stage to the
-- immediately-next stage in var('deal_stage_order'). Useful for pinpointing
-- drop-off stages.

{% set stages = var('deal_stage_order') %}

with stage_entries as (
    select
        pd.partner_id,
        pd.partner_name,
        pd.deal_id,
        lower(sd.deal_stage) as deal_stage,
        pd.is_closed_won
    from {{ ref('partner_deals') }} pd
    join {{ ref('int_deal_stage_durations') }} sd using (deal_id)
),

stage_order_lookup as (
    select * from unnest([
        {% for s in stages %}
        struct('{{ s }}' as stage_name, {{ loop.index }} as stage_order){% if not loop.last %},{% endif %}
        {% endfor %}
    ])
),

entries_with_order as (
    select
        se.*,
        sol.stage_order
    from stage_entries se
    left join stage_order_lookup sol on sol.stage_name = se.deal_stage
),

-- Per deal: max stage_order reached.
max_stage_per_deal as (
    select
        partner_id,
        partner_name,
        deal_id,
        max(stage_order) as max_stage_order,
        max(is_closed_won) as is_closed_won
    from entries_with_order
    where stage_order is not null
    group by 1, 2, 3
),

-- Per-stage aggregations.
per_stage as (
    select
        ewo.partner_id,
        ewo.partner_name,
        ewo.deal_stage,
        ewo.stage_order,
        count(distinct ewo.deal_id)                                                 as deals_entered,
        count(distinct case when ms.max_stage_order > ewo.stage_order then ewo.deal_id end) as deals_advanced,
        count(distinct case when ewo.is_closed_won then ewo.deal_id end)            as deals_closed_won
    from entries_with_order ewo
    join max_stage_per_deal ms
      on ms.partner_id = ewo.partner_id and ms.deal_id = ewo.deal_id
    where ewo.stage_order is not null
    group by 1, 2, 3, 4
),

-- Next-stage specific conversion (e.g. discovery → demo).
next_stage_conv as (
    select
        ms.partner_id,
        ms.partner_name,
        ewo.deal_stage,
        ewo.stage_order,
        count(distinct ewo.deal_id)                                                 as deals_entered_this_stage,
        count(distinct case when ms.max_stage_order >= ewo.stage_order + 1 then ewo.deal_id end) as deals_entered_next_stage
    from entries_with_order ewo
    join max_stage_per_deal ms
      on ms.partner_id = ewo.partner_id and ms.deal_id = ewo.deal_id
    where ewo.stage_order is not null
    group by 1, 2, 3, 4
)

select
    ps.partner_id,
    ps.partner_name,
    ps.deal_stage,
    ps.stage_order,
    ps.deals_entered,
    ps.deals_advanced,
    ps.deals_closed_won,
    safe_divide(ps.deals_advanced, nullif(ps.deals_entered, 0))                    as stage_conversion_rate,
    safe_divide(ps.deals_closed_won, nullif(ps.deals_entered, 0))                  as stage_to_won_rate,
    nsc.deals_entered_next_stage,
    safe_divide(nsc.deals_entered_next_stage, nullif(nsc.deals_entered_this_stage, 0)) as next_stage_conversion_rate,
    1 - safe_divide(nsc.deals_entered_next_stage, nullif(nsc.deals_entered_this_stage, 0)) as drop_off_rate
from per_stage ps
left join next_stage_conv nsc
  on ps.partner_id = nsc.partner_id
 and ps.deal_stage = nsc.deal_stage
