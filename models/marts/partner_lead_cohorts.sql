{{ config(materialized='table') }}

-- Cohort view: for each (partner, lead_created_month) cohort, how many leads
-- eventually reached each downstream stage AND how many have closed as of now.
-- Used for cohort curves on the dashboard.

with leads as (select * from {{ ref('partner_leads') }}),
     deals as (select * from {{ ref('partner_deals') }})

select
    l.partner_id,
    l.partner_name,
    l.lead_created_month                                                 as cohort_month,
    count(distinct l.contact_id)                                         as cohort_leads,
    count(distinct case when l.reached_mql then l.contact_id end)        as cohort_mqls,
    count(distinct case when l.reached_sql then l.contact_id end)        as cohort_sqls,
    count(distinct d.deal_id)                                            as cohort_deals,
    count(distinct case when d.is_closed_won then d.deal_id end)         as cohort_won_deals,
    sum(case when d.is_closed_won then d.amount else 0 end)              as cohort_won_revenue,
    avg(case when d.is_closed_won then d.time_to_close_days end)         as cohort_avg_cycle_days
from leads l
left join deals d using (contact_id)
group by 1, 2, 3
