{{ config(materialized='ephemeral') }}

-- One row per (deal, stage) with total time spent in that stage across all
-- entries (deals can re-enter a stage). Also computes total time from create
-- to close so we can amortize the "time_to_close" metric.

with history as (
    select * from {{ ref('stg_hubspot__deal_stage_history') }}
),

deals as (
    select * from {{ ref('stg_hubspot__deals') }}
),

agg_per_stage as (
    select
        deal_id,
        deal_stage,
        count(*)                                as times_entered,
        sum(seconds_in_stage)                   as total_seconds_in_stage,
        sum(seconds_in_stage) / 86400.0         as total_days_in_stage,
        min(entered_at)                         as first_entered_at,
        max(effective_exited_at)                as last_exited_at
    from history
    group by 1, 2
),

deal_totals as (
    select
        d.deal_id,
        {{ timestamp_diff_seconds('coalesce(d.deal_closed_won_at, d.deal_close_date, now())', 'd.deal_created_at') }} as seconds_to_close,
        {{ safe_divide(
            timestamp_diff_seconds('coalesce(d.deal_closed_won_at, d.deal_close_date, now())', 'd.deal_created_at'),
            '86400.0'
        ) }} as days_to_close
    from deals d
)

select
    a.deal_id,
    a.deal_stage,
    a.times_entered,
    a.total_seconds_in_stage,
    a.total_days_in_stage,
    a.first_entered_at,
    a.last_exited_at,
    t.seconds_to_close,
    t.days_to_close
from agg_per_stage a
left join deal_totals t using (deal_id)
