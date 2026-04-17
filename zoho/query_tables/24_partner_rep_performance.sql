-- Query Table name: partner_rep_performance
-- Depends on: partner_deals
--
-- One row per (partner, rep). For a rep-only view, aggregate across partners
-- at dashboard time.

SELECT
    partner_id,
    partner_name,
    deal_owner_id,
    deal_owner_name,
    deal_owner_email,
    COUNT(DISTINCT deal_id)                                                AS deals_total,
    COUNT(DISTINCT CASE WHEN is_closed_won = 1 THEN deal_id END)           AS deals_won,
    COUNT(DISTINCT CASE WHEN is_closed = 1 AND is_closed_won = 0 THEN deal_id END) AS deals_lost,
    COUNT(DISTINCT CASE WHEN is_closed = 0 THEN deal_id END)               AS deals_open,
    CASE
        WHEN COUNT(DISTINCT deal_id) > 0
        THEN COUNT(DISTINCT CASE WHEN is_closed_won = 1 THEN deal_id END) * 1.0
             / COUNT(DISTINCT deal_id)
    END                                                                    AS win_rate,
    SUM(CASE WHEN is_closed_won = 1 THEN amount ELSE 0 END)                AS revenue_closed_won,
    AVG(CASE WHEN is_closed_won = 1 THEN amount END)                       AS avg_deal_size,
    AVG(CASE WHEN is_closed_won = 1 THEN time_to_close_days END)           AS avg_cycle_days,
    AVG(CASE WHEN is_closed_won = 1 THEN sales_touches_total END)          AS avg_sales_touches
FROM "partner_deals"
GROUP BY partner_id, partner_name, deal_owner_id, deal_owner_name, deal_owner_email
