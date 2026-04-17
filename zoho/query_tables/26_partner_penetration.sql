-- Query Table name: partner_penetration
-- Depends on: partner_total_customers (uploaded CSV), partner_deals
--
-- Partner penetration = our customer count / partner's total customer count.
-- One row per (partner, as_of_date). Feeds the penetration dashboard page.

WITH our_won_customers AS (
    SELECT
        partner_id,
        partner_name,
        -- Distinct "customers" — uses deal_name as a company proxy. Replace
        -- with a company_id if you associate Deals → Companies in HubSpot.
        COUNT(DISTINCT COALESCE(NULLIF(LOWER(TRIM(deal_name)), ''),
                                CAST(contact_id AS CHAR)))  AS our_customer_count,
        MIN(deal_closed_won_at)                              AS our_first_won_at
    FROM "partner_deals"
    WHERE is_closed_won = 1
    GROUP BY partner_id, partner_name
)

SELECT
    t.partner_id,
    t.partner_name,
    t.as_of_date,
    t.total_customer_count,
    COALESCE(oc.our_customer_count, 0)                        AS our_customer_count,
    CASE WHEN t.total_customer_count > 0
         THEN COALESCE(oc.our_customer_count, 0) * 1.0 / t.total_customer_count END
                                                              AS penetration_rate,
    oc.our_first_won_at
FROM "partner_total_customers" t
LEFT JOIN our_won_customers oc ON oc.partner_id = t.partner_id
