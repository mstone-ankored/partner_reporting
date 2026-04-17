# Dashboard Structure

The tables produced by this project are designed to be consumed directly by
any BI tool (Looker, Mode, Hex, Metabase, Omni, Tableau, Power BI). Each
chart below lists the **primary table**, the **dimensions**, and the **measures**
— that's the complete binding.

Filters applied globally (top bar):
- Date range (default: trailing 12 months, filters `period_start_date` or
  `lead_created_at` / `deal_created_at` depending on chart).
- Partner (multi-select; pulls from `ref_partners.partner_name`).
- Deal owner (multi-select; pulls from `partner_rep_performance.deal_owner_name`).

---

## Page 1 — Executive Overview

One scrollable page answering "how are partners doing overall?".

| # | Chart                                   | Table                              | Dim(s)                          | Measure(s)                                                              |
|---|------------------------------------------|------------------------------------|---------------------------------|--------------------------------------------------------------------------|
| 1 | KPI tiles (big numbers)                 | `partner_summary` (all_time)       | —                               | `total_leads`, `deals_closed_won`, `revenue_closed_won`, `lead_to_won_rate` |
| 2 | Revenue trend                           | `partner_summary` (month)          | `period_start_date`             | `revenue_closed_won`                                                    |
| 3 | Leads trend, stacked by source_type     | `partner_leads`                    | `lead_created_month`, `source_type` | `count(distinct contact_id)`                                        |
| 4 | Top 10 partners by revenue              | `partner_rankings`                 | `partner_name`                  | `revenue_closed_won`, `rank_by_revenue`                                 |
| 5 | Partner quadrant scatter                | `partner_rankings`                 | `partner_name`, `volume_efficiency_quadrant` | x: `total_leads`, y: `lead_to_won_rate`, size: `revenue_closed_won` |
| 6 | Flags rail                              | `partner_rankings`                 | `partner_name`                  | `is_top_performer`, `is_high_potential`, `is_underperformer`            |

---

## Page 2 — Partner Deep-Dive (drill-down, one partner at a time)

Filtered by a single `partner_name` chosen at the top of the page.

| # | Chart                                  | Table                                    | Dim(s)                           | Measure(s)                                                    |
|---|-----------------------------------------|------------------------------------------|----------------------------------|---------------------------------------------------------------|
| 1 | Lead funnel bar chart                   | `partner_summary` (all_time)             | —                                | `total_leads`, `leads_reached_mql`, `leads_reached_sql`, `total_deals_created`, `deals_closed_won` |
| 2 | Stage-by-stage conversion              | `partner_funnel_stage_conversion`        | `deal_stage` (ordered)           | `deals_entered`, `next_stage_conversion_rate`, `drop_off_rate` |
| 3 | Drop-off call-out                       | `partner_funnel_stage_conversion`        | single row (max drop_off_rate)   | `deal_stage`, `drop_off_rate`                                 |
| 4 | Revenue / deal count by month           | `partner_summary` (month)                | `period_start_date`              | `revenue_closed_won`, `deals_closed_won`                      |
| 5 | Cohort curves (leads → wins)            | `partner_lead_cohorts`                   | `cohort_month`                   | `cohort_leads`, `cohort_won_deals`, `cohort_won_revenue`      |
| 6 | Velocity                                | `partner_summary` (all_time)             | —                                | `avg_hours_to_first_sales_touch`, `avg_days_contact_to_deal`, `avg_deal_cycle_days`, `avg_sales_touches_to_close` |
| 7 | Deal list (details)                     | `partner_deals`                          | per-row                          | `deal_name`, `deal_stage`, `amount`, `deal_owner_name`, `time_to_close_days` |

---

## Page 3 — Lead Quality

| # | Chart                                  | Table                     | Dim(s)                  | Measure(s)                                                |
|---|-----------------------------------------|---------------------------|-------------------------|-----------------------------------------------------------|
| 1 | MQL / SQL / DQ rates by partner        | `partner_summary`         | `partner_name`          | `mql_rate`, `sql_rate`, `disqualified_rate`               |
| 2 | Lead source split                       | `partner_leads`           | `partner_name`, `source_type` | `count(distinct contact_id)`                         |
| 3 | Form metadata breakdown                 | `partner_leads` (source_type = 'form') | `form_company_size`, `form_industry` | `count(distinct contact_id)` |
| 4 | Time-to-first-touch distribution        | `partner_leads`           | `partner_name`          | `avg(hours_to_first_sales_touch)`, `percentile(…)`        |

---

## Page 4 — Sales Rep Performance

| # | Chart                                  | Table                        | Dim(s)                          | Measure(s)                                  |
|---|-----------------------------------------|------------------------------|---------------------------------|---------------------------------------------|
| 1 | Wins by rep                            | `partner_rep_performance`    | `deal_owner_name`               | `deals_won`, `revenue_closed_won`           |
| 2 | Win rate by rep                         | `partner_rep_performance`    | `deal_owner_name`               | `win_rate`                                  |
| 3 | Rep × Partner matrix heatmap            | `partner_rep_performance`    | `deal_owner_name`, `partner_name` | `deals_won`, `win_rate`                  |
| 4 | Distribution of partner leads by rep    | `partner_leads` join `stg_hubspot__owners` on `contact_owner_id` | `owner_name` | `count(distinct contact_id)` |

---

## Page 5 — Partner Penetration

| # | Chart                                  | Table                  | Dim(s)                | Measure(s)                                      |
|---|-----------------------------------------|------------------------|-----------------------|-------------------------------------------------|
| 1 | Penetration by partner (latest)         | `partner_penetration` (latest as_of_date per partner) | `partner_name` | `penetration_rate`          |
| 2 | Penetration over time                   | `partner_penetration`  | `as_of_date`, `partner_name` | `penetration_rate`                       |
| 3 | Absolute counts                         | `partner_penetration`  | `partner_name`        | `total_customer_count`, `our_customer_count`    |

---

## How the dashboard stays live

- The orchestrator (dbt Cloud / Airflow / GH Actions) runs `dbt build` on a
  schedule — typically every 30–60 min.
- `partner_leads` and `partner_deals` are incremental with merge on
  `contact_id` / `deal_id`, so runs are cheap even as HubSpot grows.
- The BI tool queries the mart tables directly (no extra transformation in
  BI). If your BI tool supports cache invalidation on table refresh, wire it
  to dbt's `post-hook` or run artifacts for a sub-minute freshness loop.

## Minimum BI-side customization

Most charts above work out of the box. The one exception is the "drop-off
call-out" on Page 2, which needs a BI-side expression like:

```
argmax(drop_off_rate, deal_stage) WHERE partner_name = <selected>
```

or the equivalent in your tool's modeling language.
