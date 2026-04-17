-- Query Table name: int_deal_sales_touches
-- Depends on: stg_deals, int_deal_primary_contact, stg_engagements,
--             stg_engagement_contacts
--
-- Counts of sales touches per deal (primary-contact engagements within the
-- deal's lifetime).

WITH deal_window AS (
    SELECT
        d.deal_id,
        d.deal_created_at,
        COALESCE(d.deal_close_date, NOW()) AS deal_end_at,
        pc.primary_contact_id
    FROM "stg_deals"                 d
    LEFT JOIN "int_deal_primary_contact" pc ON pc.deal_id = d.deal_id
),
joined AS (
    SELECT
        dw.deal_id,
        e.engagement_id,
        e.engagement_type,
        e.email_direction
    FROM deal_window                dw
    JOIN "stg_engagement_contacts"  ec ON ec.contact_id    = dw.primary_contact_id
    JOIN "stg_engagements"          e  ON e.engagement_id = ec.engagement_id
                                     AND e.engaged_at BETWEEN dw.deal_created_at AND dw.deal_end_at
)

SELECT
    deal_id,
    COUNT(DISTINCT engagement_id)                                                AS total_touches,
    COUNT(DISTINCT CASE WHEN engagement_type = 'email' AND email_direction = 'outgoing'
                        THEN engagement_id END)                                  AS outbound_emails,
    COUNT(DISTINCT CASE WHEN engagement_type = 'call'    THEN engagement_id END) AS calls,
    COUNT(DISTINCT CASE WHEN engagement_type = 'meeting' THEN engagement_id END) AS meetings
FROM joined
GROUP BY deal_id
