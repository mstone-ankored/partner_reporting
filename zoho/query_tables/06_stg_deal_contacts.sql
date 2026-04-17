-- Query Table name: stg_deal_contacts
-- Depends on: Deal Contacts (many-to-many deal ↔ contact table)

SELECT
    "Deal Id"                                               AS deal_id,
    "Contact Id"                                            AS contact_id,
    CASE
        WHEN "Is Primary" = 'true' OR "Is Primary" = '1' THEN 1
        ELSE 0
    END                                                     AS is_primary_contact
FROM "Deal Contacts"
WHERE "Deal Id"    IS NOT NULL
  AND "Contact Id" IS NOT NULL
