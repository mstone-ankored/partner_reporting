-- Query Table name: int_deal_primary_contact
-- Depends on: stg_deal_contacts, stg_contacts
--
-- Resolves one primary contact per deal:
--   1. Any association flagged is_primary = 1
--   2. Otherwise the earliest-created associated contact

WITH joined AS (
    SELECT
        dc.deal_id,
        dc.contact_id,
        dc.is_primary_contact,
        c.contact_created_at,
        ROW_NUMBER() OVER (
            PARTITION BY dc.deal_id
            ORDER BY
                CASE WHEN dc.is_primary_contact = 1 THEN 0 ELSE 1 END ASC,
                c.contact_created_at ASC,
                dc.contact_id ASC
        ) AS rn
    FROM "stg_deal_contacts" dc
    LEFT JOIN "stg_contacts"  c ON c.contact_id = dc.contact_id
)

SELECT
    deal_id,
    contact_id AS primary_contact_id
FROM joined
WHERE rn = 1
