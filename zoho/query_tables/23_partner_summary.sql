-- Query Table name: partner_summary
-- Depends on: partner_leads, partner_deals, partner_penetration
--
-- Aggregated KPI table: one row per (partner, period_type, period_start).
-- period_type ∈ ('month', 'quarter', 'all_time'). Powers the headline
-- dashboard charts (revenue trends, conversion rates, partner contribution).

WITH

-- Union the period grains we need — lead-created and deal-created-or-close.
partner_periods AS (
    SELECT partner_id, partner_name, 'month'   AS period_type, lead_created_month   AS period_start_date FROM "partner_leads"
    UNION SELECT partner_id, partner_name, 'quarter', lead_created_quarter FROM "partner_leads"
    UNION SELECT partner_id, partner_name, 'month',   deal_created_month   FROM "partner_deals"
    UNION SELECT partner_id, partner_name, 'quarter', deal_created_quarter FROM "partner_deals"
    UNION SELECT partner_id, partner_name, 'month',   deal_close_month     FROM "partner_deals" WHERE deal_close_month IS NOT NULL
    UNION SELECT partner_id, partner_name, 'quarter', deal_close_quarter   FROM "partner_deals" WHERE deal_close_quarter IS NOT NULL
    UNION SELECT partner_id, partner_name, 'all_time', NULL FROM "partner_leads"
    UNION SELECT partner_id, partner_name, 'all_time', NULL FROM "partner_deals"
),

lead_metrics AS (
    SELECT
        pp.partner_id,
        pp.partner_name,
        pp.period_type,
        pp.period_start_date,
        COUNT(DISTINCT l.contact_id)                                                            AS total_leads,
        COUNT(DISTINCT CASE WHEN l.source_type = 'partner_email' THEN l.contact_id END)         AS leads_from_partner_email,
        COUNT(DISTINCT CASE WHEN l.source_type = 'form'          THEN l.contact_id END)         AS leads_from_form,
        COUNT(DISTINCT CASE WHEN l.reached_mql     = 1 THEN l.contact_id END)                   AS leads_reached_mql,
        COUNT(DISTINCT CASE WHEN l.reached_sql     = 1 THEN l.contact_id END)                   AS leads_reached_sql,
        COUNT(DISTINCT CASE WHEN l.is_disqualified = 1 THEN l.contact_id END)                   AS leads_disqualified,
        AVG(l.hours_to_first_sales_touch)                                                       AS avg_hours_to_first_sales_touch
    FROM partner_periods pp
    LEFT JOIN "partner_leads" l
      ON l.partner_id = pp.partner_id
     AND (
           pp.period_type = 'all_time'
        OR (pp.period_type = 'month'   AND l.lead_created_month   = pp.period_start_date)
        OR (pp.period_type = 'quarter' AND l.lead_created_quarter = pp.period_start_date)
         )
    GROUP BY pp.partner_id, pp.partner_name, pp.period_type, pp.period_start_date
),

deal_create_metrics AS (
    SELECT
        pp.partner_id,
        pp.partner_name,
        pp.period_type,
        pp.period_start_date,
        COUNT(DISTINCT d.deal_id)       AS total_deals_created,
        AVG(d.days_contact_to_deal)     AS avg_days_contact_to_deal
    FROM partner_periods pp
    LEFT JOIN "partner_deals" d
      ON d.partner_id = pp.partner_id
     AND (
           pp.period_type = 'all_time'
        OR (pp.period_type = 'month'   AND d.deal_created_month   = pp.period_start_date)
        OR (pp.period_type = 'quarter' AND d.deal_created_quarter = pp.period_start_date)
         )
    GROUP BY pp.partner_id, pp.partner_name, pp.period_type, pp.period_start_date
),

deal_close_metrics AS (
    SELECT
        pp.partner_id,
        pp.partner_name,
        pp.period_type,
        pp.period_start_date,
        COUNT(DISTINCT CASE WHEN d.is_closed_won = 1 THEN d.deal_id END)                AS deals_closed_won,
        COUNT(DISTINCT CASE WHEN d.is_closed = 1 AND d.is_closed_won = 0 THEN d.deal_id END) AS deals_closed_lost,
        SUM(CASE WHEN d.is_closed_won = 1 THEN d.amount ELSE 0 END)                     AS revenue_closed_won,
        AVG(CASE WHEN d.is_closed_won = 1 THEN d.amount END)                            AS avg_deal_size,
        AVG(CASE WHEN d.is_closed_won = 1 THEN d.time_to_close_days END)                AS avg_deal_cycle_days,
        AVG(CASE WHEN d.is_closed_won = 1 THEN d.sales_touches_total END)               AS avg_sales_touches_to_close
    FROM partner_periods pp
    LEFT JOIN "partner_deals" d
      ON d.partner_id = pp.partner_id
     AND (
           pp.period_type = 'all_time'
        OR (pp.period_type = 'month'   AND d.deal_close_month   = pp.period_start_date)
        OR (pp.period_type = 'quarter' AND d.deal_close_quarter = pp.period_start_date)
         )
    GROUP BY pp.partner_id, pp.partner_name, pp.period_type, pp.period_start_date
),

-- All-partner totals per period for share-of-total calcs.
period_totals AS (
    SELECT
        period_type,
        period_start_date,
        SUM(total_leads)            AS all_partners_leads,
        SUM(total_deals_created)    AS all_partners_deals,
        SUM(revenue_closed_won)     AS all_partners_revenue
    FROM (
        SELECT lm.period_type, lm.period_start_date, lm.total_leads,
               dcm.total_deals_created, dclm.revenue_closed_won
        FROM lead_metrics         lm
        LEFT JOIN deal_create_metrics dcm
               ON dcm.partner_id = lm.partner_id AND dcm.period_type = lm.period_type
              AND (dcm.period_start_date = lm.period_start_date
                   OR (dcm.period_start_date IS NULL AND lm.period_start_date IS NULL))
        LEFT JOIN deal_close_metrics  dclm
               ON dclm.partner_id = lm.partner_id AND dclm.period_type = lm.period_type
              AND (dclm.period_start_date = lm.period_start_date
                   OR (dclm.period_start_date IS NULL AND lm.period_start_date IS NULL))
    ) x
    GROUP BY period_type, period_start_date
),

-- Latest penetration snapshot (one row per partner).
latest_penetration AS (
    SELECT
        partner_id,
        partner_name,
        penetration_rate,
        total_customer_count,
        our_customer_count,
        ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY as_of_date DESC) AS rn
    FROM "partner_penetration"
)

SELECT
    pp.partner_id,
    pp.partner_name,
    pp.period_type,
    pp.period_start_date,

    COALESCE(lm.total_leads, 0)                                                AS total_leads,
    COALESCE(lm.leads_from_partner_email, 0)                                   AS leads_from_partner_email,
    COALESCE(lm.leads_from_form, 0)                                            AS leads_from_form,

    COALESCE(lm.leads_reached_mql, 0)                                          AS leads_reached_mql,
    COALESCE(lm.leads_reached_sql, 0)                                          AS leads_reached_sql,
    COALESCE(lm.leads_disqualified, 0)                                         AS leads_disqualified,
    CASE WHEN lm.total_leads > 0 THEN lm.leads_reached_mql  * 1.0 / lm.total_leads END AS mql_rate,
    CASE WHEN lm.total_leads > 0 THEN lm.leads_reached_sql  * 1.0 / lm.total_leads END AS sql_rate,
    CASE WHEN lm.total_leads > 0 THEN lm.leads_disqualified * 1.0 / lm.total_leads END AS disqualified_rate,

    COALESCE(dcm.total_deals_created, 0)                                       AS total_deals_created,
    CASE WHEN lm.total_leads > 0
         THEN dcm.total_deals_created * 1.0 / lm.total_leads END                AS lead_to_deal_rate,
    COALESCE(dclm.deals_closed_won, 0)                                         AS deals_closed_won,
    COALESCE(dclm.deals_closed_lost, 0)                                        AS deals_closed_lost,
    CASE WHEN dcm.total_deals_created > 0
         THEN dclm.deals_closed_won * 1.0 / dcm.total_deals_created END         AS deal_to_won_rate,
    CASE WHEN lm.total_leads > 0
         THEN dclm.deals_closed_won * 1.0 / lm.total_leads END                  AS lead_to_won_rate,

    lm.avg_hours_to_first_sales_touch,
    dcm.avg_days_contact_to_deal,
    dclm.avg_deal_cycle_days,
    dclm.avg_sales_touches_to_close,

    COALESCE(dclm.revenue_closed_won, 0)                                       AS revenue_closed_won,
    dclm.avg_deal_size,
    CASE WHEN lm.total_leads > 0
         THEN dclm.revenue_closed_won / lm.total_leads END                      AS revenue_per_lead,
    CASE WHEN dclm.deals_closed_won > 0
         THEN dclm.revenue_closed_won / dclm.deals_closed_won END               AS revenue_per_closed_won_deal,

    CASE WHEN pt.all_partners_leads > 0
         THEN lm.total_leads * 1.0 / pt.all_partners_leads END                  AS share_of_total_leads,
    CASE WHEN pt.all_partners_deals > 0
         THEN dcm.total_deals_created * 1.0 / pt.all_partners_deals END         AS share_of_total_deals,
    CASE WHEN pt.all_partners_revenue > 0
         THEN dclm.revenue_closed_won / pt.all_partners_revenue END             AS share_of_total_revenue,

    lp.total_customer_count                                                    AS partner_total_customers,
    lp.our_customer_count                                                      AS our_customers_at_partner,
    lp.penetration_rate                                                        AS partner_penetration_rate

FROM partner_periods        pp
LEFT JOIN lead_metrics        lm   ON lm.partner_id   = pp.partner_id
                                   AND lm.period_type = pp.period_type
                                   AND (lm.period_start_date = pp.period_start_date
                                        OR (lm.period_start_date IS NULL AND pp.period_start_date IS NULL))
LEFT JOIN deal_create_metrics dcm  ON dcm.partner_id   = pp.partner_id
                                   AND dcm.period_type = pp.period_type
                                   AND (dcm.period_start_date = pp.period_start_date
                                        OR (dcm.period_start_date IS NULL AND pp.period_start_date IS NULL))
LEFT JOIN deal_close_metrics  dclm ON dclm.partner_id   = pp.partner_id
                                   AND dclm.period_type = pp.period_type
                                   AND (dclm.period_start_date = pp.period_start_date
                                        OR (dclm.period_start_date IS NULL AND pp.period_start_date IS NULL))
LEFT JOIN period_totals       pt   ON pt.period_type  = pp.period_type
                                   AND (pt.period_start_date = pp.period_start_date
                                        OR (pt.period_start_date IS NULL AND pp.period_start_date IS NULL))
LEFT JOIN latest_penetration  lp   ON lp.partner_id = pp.partner_id AND lp.rn = 1
