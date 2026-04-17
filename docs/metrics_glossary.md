# Metrics Glossary

Every metric produced by the marts, with its exact formula. Use this as the
single source of truth when defining KPIs in the BI tool — do not redefine
them BI-side unless you also update this doc.

## Lead volume & source

| Metric                   | Formula                                                               | Table             |
|--------------------------|-----------------------------------------------------------------------|-------------------|
| `total_leads`            | `count(distinct contact_id)` in `partner_leads`                       | `partner_summary` |
| `leads_from_partner_email` | above, filtered `source_type = 'partner_email'`                     | `partner_summary` |
| `leads_from_form`        | above, filtered `source_type = 'form'`                                | `partner_summary` |

## Lead quality

| Metric                  | Formula                                                  | Table             |
|-------------------------|-----------------------------------------------------------|-------------------|
| `mql_rate`              | `leads_reached_mql / total_leads`                        | `partner_summary` |
| `sql_rate`              | `leads_reached_sql / total_leads`                        | `partner_summary` |
| `disqualified_rate`     | `leads_disqualified / total_leads`                       | `partner_summary` |

## Funnel conversion

| Metric                     | Formula                                                 | Table             |
|----------------------------|---------------------------------------------------------|-------------------|
| `lead_to_deal_rate`        | `total_deals_created / total_leads`                     | `partner_summary` |
| `deal_to_won_rate`         | `deals_closed_won / total_deals_created`                | `partner_summary` |
| `lead_to_won_rate`         | `deals_closed_won / total_leads`                        | `partner_summary` |
| `stage_conversion_rate`    | `deals_advanced / deals_entered` (per stage)            | `partner_funnel_stage_conversion` |
| `next_stage_conversion_rate` | `deals_entered_next_stage / deals_entered_this_stage` | `partner_funnel_stage_conversion` |
| `drop_off_rate`            | `1 - next_stage_conversion_rate`                        | `partner_funnel_stage_conversion` |

## Velocity & efficiency

| Metric                           | Formula                                                 | Table             |
|----------------------------------|---------------------------------------------------------|-------------------|
| `avg_hours_to_first_sales_touch` | `avg(first_sales_touch_at - contact_created_at)` in hours | `partner_summary` |
| `avg_days_contact_to_deal`       | `avg(deal_created_at - contact_created_at)` in days     | `partner_summary` |
| `avg_deal_cycle_days`            | `avg(closed_won_at - deal_created_at)` in days for won deals | `partner_summary` |
| `avg_sales_touches_to_close`     | `avg(sales_touches_total)` for closed-won deals         | `partner_summary` |

## Revenue

| Metric                         | Formula                                                    | Table             |
|--------------------------------|-------------------------------------------------------------|-------------------|
| `revenue_closed_won`           | `sum(amount)` where `is_closed_won = true`                  | `partner_summary` |
| `avg_deal_size`                | `avg(amount)` where `is_closed_won = true`                  | `partner_summary` |
| `median_deal_size`             | median of `amount` where `is_closed_won = true`             | `partner_summary` |
| `revenue_per_lead`             | `revenue_closed_won / total_leads`                          | `partner_summary` |
| `revenue_per_closed_won_deal`  | `revenue_closed_won / deals_closed_won`                     | `partner_summary` |

## Partner share

| Metric                    | Formula                                                        | Table             |
|---------------------------|----------------------------------------------------------------|-------------------|
| `share_of_total_leads`    | partner's `total_leads` / all partners' `total_leads`           | `partner_summary` |
| `share_of_total_deals`    | partner's `total_deals_created` / all partners' total           | `partner_summary` |
| `share_of_total_revenue`  | partner's `revenue_closed_won` / all partners' total            | `partner_summary` |

## Partner penetration

| Metric                  | Formula                                                         | Table                 |
|-------------------------|-----------------------------------------------------------------|-----------------------|
| `penetration_rate`      | `our_customer_count / total_customer_count`                     | `partner_penetration` |

## Classification (rankings)

| Flag                              | Rule                                                                |
|-----------------------------------|---------------------------------------------------------------------|
| `is_top_performer`                | top 5 by revenue OR `volume_efficiency_quadrant = high/high`        |
| `is_high_potential`               | `volume_efficiency_quadrant = low_volume_high_conversion`           |
| `is_underperformer`               | above-median volume AND lead-to-won rate < ½ × median               |
| `volume_efficiency_quadrant`      | median-split on (`total_leads`, `lead_to_won_rate`)                 |
