# Building the dashboards in Zoho Analytics

Once the tables are syncing (see `zoho_analytics_setup.md`), wire up the
dashboards. This doc maps the generic layout in `dashboard_structure.md` to
Zoho-specific widget types and formula columns.

## 1. Reports vs Dashboards in Zoho

- **Report** = a single chart or pivot built on top of a table.
- **Dashboard** = a canvas that embeds multiple reports + filters.

Build each report once; then drop it onto the dashboards below.

## 2. Recommended workspace structure

```
Workspace: Partner Performance
├── Data Sources (hidden folder)
│   └── 9 mart tables from sync
├── Reports
│   ├── Executive
│   ├── Partner Deep-Dive
│   ├── Lead Quality
│   ├── Sales Rep Performance
│   └── Partner Penetration
└── Dashboards
    ├── Executive Overview
    ├── Partner 360
    ├── Lead Quality
    ├── Rep Performance
    └── Penetration
```

## 3. Global user filters

Add these as **User Filters** on each dashboard so a single control filters
every report on the page:

| Filter name       | Table / column                       | Type            |
|-------------------|--------------------------------------|-----------------|
| Partner           | `partner_rankings.partner_name`      | Multi-select    |
| Date range        | `partner_summary.period_start_date`  | Date-range      |
| Period grain      | `partner_summary.period_type`        | Single-select (month / quarter / all_time) |
| Deal owner        | `partner_rep_performance.deal_owner_name` | Multi-select |

## 4. Report recipes

### 4.1 Executive Overview

1. **KPI tiles** (4 tiles)
   - Source: `partner_summary` filtered `period_type = 'all_time'`
   - Widget: **KPI Widget**
   - Metrics: `total_leads`, `deals_closed_won`, `revenue_closed_won`,
     `lead_to_won_rate` (format as percent).

2. **Revenue trend**
   - Source: `partner_summary` filtered `period_type = 'month'`
   - Widget: **Line chart**
   - X: `period_start_date`; Y: `sum(revenue_closed_won)`; Series:
     `partner_name` (optional — turns into a stacked line).

3. **Leads trend by source**
   - Source: `partner_leads`
   - Widget: **Stacked column**
   - X: `lead_created_month`; Y: `count(distinct contact_id)`; Stack by: `source_type`.

4. **Top-10 partners by revenue**
   - Source: `partner_rankings`
   - Widget: **Bar chart (horizontal)**
   - Y: `partner_name` sorted by `revenue_closed_won desc`, limit 10.

5. **Partner quadrant scatter**
   - Source: `partner_rankings`
   - Widget: **Scatter chart**
   - X: `total_leads`; Y: `lead_to_won_rate`; Size: `revenue_closed_won`;
     Color: `volume_efficiency_quadrant`.
   - Add reference lines at X = median(total_leads), Y = median(lead_to_won_rate)
     to visually show the quadrant split.

### 4.2 Partner Deep-Dive

Filter the whole dashboard to a single `partner_name`.

1. **Funnel**
   - Source: a union/formula report on `partner_summary (all_time)` exposing
     the 5 funnel counts as rows.
   - Widget: **Funnel chart**
   - Steps: `total_leads → leads_reached_mql → leads_reached_sql →
     total_deals_created → deals_closed_won`.

2. **Stage-by-stage conversion**
   - Source: `partner_funnel_stage_conversion`
   - Widget: **Combo chart**
   - X: `deal_stage` (sort by `stage_order` asc); Bars: `deals_entered`;
     Line: `next_stage_conversion_rate`.

3. **Drop-off call-out**
   - Source: `partner_funnel_stage_conversion`
   - Widget: **KPI Widget** with a custom formula:
     ```
     MAX (drop_off_rate) over (partition by partner_id)
     ```
   - Zoho does not have a native `argmax`; build it as a **Query Table**:
     ```sql
     select partner_name, deal_stage, drop_off_rate
     from "partner_funnel_stage_conversion"
     where (partner_id, drop_off_rate) in (
       select partner_id, max(drop_off_rate)
       from "partner_funnel_stage_conversion"
       group by partner_id
     )
     ```

4. **Cohort chart**
   - Source: `partner_lead_cohorts`
   - Widget: **Heatmap**
   - Rows: `cohort_month`; Cols: months since cohort (requires a derived
     column — see §5); Cells: `cohort_won_deals / cohort_leads`.

### 4.3 Lead Quality

1. **MQL / SQL / DQ rates by partner**
   - Source: `partner_summary (all_time)`
   - Widget: **Grouped bar chart** — X: `partner_name`, Y: `mql_rate`,
     `sql_rate`, `disqualified_rate`.

2. **Source split**
   - Source: `partner_leads`
   - Widget: **100% stacked column** — X: `partner_name`; Y: count; Stack: `source_type`.

3. **Time-to-touch**
   - Source: `partner_leads`
   - Widget: **Box plot** on `hours_to_first_sales_touch` grouped by `partner_name`.

### 4.4 Sales Rep Performance

1. **Wins by rep** — bar chart from `partner_rep_performance`,
   X: `deal_owner_name`, Y: `sum(deals_won)`.
2. **Win rate by rep** — bar chart, Y: `win_rate` (format %).
3. **Rep × Partner heatmap** — from `partner_rep_performance`,
   rows: `deal_owner_name`, cols: `partner_name`, cell: `win_rate`.

### 4.5 Penetration

1. **Latest penetration per partner**
   - Build a **Query Table** first:
     ```sql
     select partner_name, total_customer_count, our_customer_count, penetration_rate
     from "partner_penetration"
     where (partner_id, as_of_date) in (
       select partner_id, max(as_of_date) from "partner_penetration" group by partner_id
     )
     ```
   - Widget: **Bar chart** sorted by `penetration_rate desc`, format as %.

2. **Penetration over time**
   - Source: `partner_penetration` directly.
   - Widget: **Line chart** — X: `as_of_date`, Y: `penetration_rate`,
     Series: `partner_name`.

## 5. Useful formula columns

Add these on the data source (not per-report) so every chart can reuse them.

| Column name              | Formula                                           | Table                |
|--------------------------|---------------------------------------------------|----------------------|
| `month_label`            | `TODATE(period_start_date, 'yyyy-MM')`            | `partner_summary`    |
| `revenue_k`              | `revenue_closed_won / 1000`                       | `partner_summary`    |
| `days_to_won`            | `DIFF_DAYS(deal_created_at, deal_closed_won_at)`  | `partner_deals`      |
| `months_since_cohort`    | `DIFF_MONTHS(cohort_month, deal_close_month)`     | `partner_lead_cohorts` + join |

## 6. Automation

- Use **Email Schedules** to mail PDF exports of the Executive Overview every
  Monday 8am to the exec team.
- Use **Alerts** on `partner_summary.disqualified_rate` to ping Slack when
  any partner's DQ rate crosses a threshold (e.g. >50% month-over-month).
- Use **Themes** to set a consistent palette per partner (match `partner_tier`
  from `ref_partners`).

## 7. Sharing

- Create a **View-only group** for stakeholders; grant workspace access at
  "View" level only.
- For external partners, use **Permalinks** (Share → Create Permalink) with
  row-level filters so each partner sees only their own rows.
