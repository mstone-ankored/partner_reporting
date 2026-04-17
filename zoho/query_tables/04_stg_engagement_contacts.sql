-- Query Table name: stg_engagement_contacts
-- Depends on: Engagement Contacts (the many-to-many association table)

SELECT
    "Engagement Id"                                         AS engagement_id,
    "Contact Id"                                            AS contact_id
FROM "Engagement Contacts"
WHERE "Engagement Id" IS NOT NULL
  AND "Contact Id"     IS NOT NULL
