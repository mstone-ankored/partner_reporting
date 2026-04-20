{{ config(materialized='table') }}

-- partner_summary: aggregated KPI table, one row per (partner, period_type, period_start_date).
-- period_type is 'month' or 'quarter'. A grand-total row is also included as
-- period_type = 'all_time' with period_start_date = null.
--
-- This table is what the dashboard reads for headline KPIs, trend charts, and
-- partner ranking views. It is intentionally wide; the dashboard can pick a
-- subset of columns per widget.

with leads as (
    select * from {{ ref('partner_leads') }}
),

deals as (
    select * from {{ ref('partner_deals') }}
),

deals_cycle as (
    -- Closed-won deals only — used for cycle-time and median deal size.
    select *
    from {{ ref('partner_deals') }}
    where is_closed_won = true
),

penetration as (
    select * from {{ ref('partner_penetration') }}
),

-- -----------------------------------------------------------------------------
-- Helper: generate one row per (partner, period_type, period_start_date) that
-- we need to aggregate over, by unioning the period grain of leads and deals.
-- -----------------------------------------------------------------------------
partner_periods as (
    select partner_id, partner_name, 'month'   as period_type, lead_created_month   as period_start_date from leads
    union
    select partner_id, partner_name, 'quarter' as period_type, lead_created_quarter as period_start_date from leads
    union
    select partner_id, partner_name, 'month'   as period_type, deal_created_month   as period_start_date from deals
    union
    select partner_id, partner_name, 'quarter' as period_type, deal_created_quarter as period_start_date from deals
    union
    select partner_id, partner_name, 'month'   as period_type, deal_close_month     as period_start_date from deals where deal_close_month is not null
    union
    select partner_id, partner_name, 'quarter' as period_type, deal_close_quarter   as period_start_date from deals where deal_close_quarter is not null
    union
    select partner_id, partner_name, 'all_time' as period_type, cast(null as date) as period_start_date from leads
    union
    select partner_id, partner_name, 'all_time' as period_type, cast(null as date) as period_start_date from deals
),

-- -----------------------------------------------------------------------------
-- Lead-volume metrics, scoped by lead_created_date ∈ period.
-- -----------------------------------------------------------------------------
lead_metrics as (
    select
        pp.partner_id,
        pp.partner_name,
        pp.period_type,
        pp.period_start_date,
        count(distinct l.contact_id)                                          as total_leads,
        count(distinct case when l.source_type = 'partner_email' then l.contact_id end) as leads_from_partner_email,
        count(distinct case when l.source_type = 'form'          then l.contact_id end) as leads_from_form,
        count(distinct case when l.reached_mql     then l.contact_id end)     as leads_reached_mql,
        count(distinct case when l.reached_sql     then l.contact_id end)     as leads_reached_sql,
        count(distinct case when l.is_disqualified then l.contact_id end)     as leads_disqualified,
        avg(l.hours_to_first_sales_touch)                                     as avg_hours_to_first_sales_touch
    from partner_periods pp
    left join leads l
      on l.partner_id = pp.partner_id
     and (
            pp.period_type = 'all_time'
         or (pp.period_type = 'month'   and l.lead_created_month   = pp.period_start_date)
         or (pp.period_type = 'quarter' and l.lead_created_quarter = pp.period_start_date)
         )
    group by 1, 2, 3, 4
),

-- -----------------------------------------------------------------------------
-- Deal-volume metrics, scoped by deal_created_date ∈ period. Close metrics use
-- close_date ∈ period (separate CTE), joined back at the end.
-- -----------------------------------------------------------------------------
deal_create_metrics as (
    select
        pp.partner_id,
        pp.partner_name,
        pp.period_type,
        pp.period_start_date,
        count(distinct d.deal_id)                                             as total_deals_created,
        avg(d.days_contact_to_deal)                                           as avg_days_contact_to_deal
    from partner_periods pp
    left join deals d
      on d.partner_id = pp.partner_id
     and (
            pp.period_type = 'all_time'
         or (pp.period_type = 'month'   and d.deal_created_month   = pp.period_start_date)
         or (pp.period_type = 'quarter' and d.deal_created_quarter = pp.period_start_date)
         )
    group by 1, 2, 3, 4
),

deal_close_metrics as (
    select
        pp.partner_id,
        pp.partner_name,
        pp.period_type,
        pp.period_start_date,
        count(distinct case when d.is_closed_won then d.deal_id end)          as deals_closed_won,
        count(distinct case when d.is_closed and not d.is_closed_won then d.deal_id end) as deals_closed_lost,
        sum(case when d.is_closed_won then d.amount else 0 end)               as revenue_closed_won,
        avg(case when d.is_closed_won then d.amount end)                      as avg_deal_size,
        {{ approx_median('case when d.is_closed_won then d.amount end') }}    as median_deal_size,
        avg(case when d.is_closed_won then d.time_to_close_days end)          as avg_deal_cycle_days,
        avg(case when d.is_closed_won then d.sales_touches_total end)         as avg_sales_touches_to_close
    from partner_periods pp
    left join deals d
      on d.partner_id = pp.partner_id
     and (
            pp.period_type = 'all_time'
         or (pp.period_type = 'month'   and d.deal_close_month   = pp.period_start_date)
         or (pp.period_type = 'quarter' and d.deal_close_quarter = pp.period_start_date)
         )
    group by 1, 2, 3, 4
),

-- -----------------------------------------------------------------------------
-- Global totals per period, used to compute each partner's share of total.
-- -----------------------------------------------------------------------------
period_totals as (
    select
        period_type,
        period_start_date,
        sum(total_leads)            as all_partners_leads,
        sum(total_deals_created)    as all_partners_deals,
        sum(revenue_closed_won)     as all_partners_revenue
    from (
        select lm.period_type, lm.period_start_date, lm.total_leads,
               dcm.total_deals_created, dclm.revenue_closed_won
        from lead_metrics lm
        left join deal_create_metrics dcm using (partner_id, partner_name, period_type, period_start_date)
        left join deal_close_metrics  dclm using (partner_id, partner_name, period_type, period_start_date)
    ) joined
    group by 1, 2
),

-- -----------------------------------------------------------------------------
-- Latest partner penetration snapshot (as of the most recent as_of_date
-- available in partner_total_customers).
-- -----------------------------------------------------------------------------
latest_penetration as (
    select partner_id, partner_name, penetration_rate, total_customer_count, our_customer_count
    from (
        select
            penetration.*,
            row_number() over (partition by partner_id order by as_of_date desc) as _rn
        from penetration
    ) ranked
    where _rn = 1
)

select
    pp.partner_id,
    pp.partner_name,
    pp.period_type,
    pp.period_start_date,

    -- Volume
    coalesce(lm.total_leads, 0)                                              as total_leads,
    coalesce(lm.leads_from_partner_email, 0)                                 as leads_from_partner_email,
    coalesce(lm.leads_from_form, 0)                                          as leads_from_form,

    -- Quality
    coalesce(lm.leads_reached_mql, 0)                                        as leads_reached_mql,
    coalesce(lm.leads_reached_sql, 0)                                        as leads_reached_sql,
    coalesce(lm.leads_disqualified, 0)                                       as leads_disqualified,
    {{ safe_divide('lm.leads_reached_mql',  'lm.total_leads') }}              as mql_rate,
    {{ safe_divide('lm.leads_reached_sql',  'lm.total_leads') }}              as sql_rate,
    {{ safe_divide('lm.leads_disqualified', 'lm.total_leads') }}              as disqualified_rate,

    -- Funnel
    coalesce(dcm.total_deals_created, 0)                                     as total_deals_created,
    {{ safe_divide('dcm.total_deals_created', 'lm.total_leads') }}            as lead_to_deal_rate,
    coalesce(dclm.deals_closed_won, 0)                                       as deals_closed_won,
    coalesce(dclm.deals_closed_lost, 0)                                      as deals_closed_lost,
    {{ safe_divide('dclm.deals_closed_won', 'dcm.total_deals_created') }}    as deal_to_won_rate,
    {{ safe_divide('dclm.deals_closed_won', 'lm.total_leads') }}              as lead_to_won_rate,

    -- Velocity / efficiency
    lm.avg_hours_to_first_sales_touch,
    dcm.avg_days_contact_to_deal,
    dclm.avg_deal_cycle_days,
    dclm.avg_sales_touches_to_close,

    -- Revenue
    coalesce(dclm.revenue_closed_won, 0)                                     as revenue_closed_won,
    dclm.avg_deal_size,
    dclm.median_deal_size,
    {{ safe_divide('dclm.revenue_closed_won', 'lm.total_leads') }}            as revenue_per_lead,
    {{ safe_divide('dclm.revenue_closed_won', 'dclm.deals_closed_won') }}    as revenue_per_closed_won_deal,

    -- Share of total (partner contribution)
    {{ safe_divide('lm.total_leads',          'pt.all_partners_leads') }}    as share_of_total_leads,
    {{ safe_divide('dcm.total_deals_created', 'pt.all_partners_deals') }}    as share_of_total_deals,
    {{ safe_divide('dclm.revenue_closed_won', 'pt.all_partners_revenue') }}  as share_of_total_revenue,

    -- Penetration (static snapshot — same value across periods)
    lp.total_customer_count                                                  as partner_total_customers,
    lp.our_customer_count                                                    as our_customers_at_partner,
    lp.penetration_rate                                                      as partner_penetration_rate

from partner_periods pp
left join lead_metrics        lm   using (partner_id, partner_name, period_type, period_start_date)
left join deal_create_metrics dcm  using (partner_id, partner_name, period_type, period_start_date)
left join deal_close_metrics  dclm using (partner_id, partner_name, period_type, period_start_date)
left join period_totals       pt   using (period_type, period_start_date)
left join latest_penetration  lp   using (partner_id, partner_name)
