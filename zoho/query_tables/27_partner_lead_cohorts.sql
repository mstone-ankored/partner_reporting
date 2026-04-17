-- Query Table name: partner_lead_cohorts
-- Depends on: partner_leads, partner_deals
--
-- For each (partner, lead_creation_month) cohort, how many leads eventually
-- reached each downstream stage and how much revenue they produced.

SELECT
    l.partner_id,
    l.partner_name,
    l.lead_created_month                                                  AS cohort_month,
    COUNT(DISTINCT l.contact_id)                                          AS cohort_leads,
    COUNT(DISTINCT CASE WHEN l.reached_mql = 1 THEN l.contact_id END)     AS cohort_mqls,
    COUNT(DISTINCT CASE WHEN l.reached_sql = 1 THEN l.contact_id END)     AS cohort_sqls,
    COUNT(DISTINCT d.deal_id)                                             AS cohort_deals,
    COUNT(DISTINCT CASE WHEN d.is_closed_won = 1 THEN d.deal_id END)      AS cohort_won_deals,
    SUM(CASE WHEN d.is_closed_won = 1 THEN d.amount ELSE 0 END)           AS cohort_won_revenue,
    AVG(CASE WHEN d.is_closed_won = 1 THEN d.time_to_close_days END)      AS cohort_avg_cycle_days
FROM      "partner_leads" l
LEFT JOIN "partner_deals" d ON d.contact_id = l.contact_id
GROUP BY l.partner_id, l.partner_name, l.lead_created_month
