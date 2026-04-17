-- Query Table name: stg_engagements
-- Depends on: Engagements (from HubSpot connector)
--
-- Zoho's HubSpot connector usually lands one row per engagement with a
-- many-to-many `Engagement Contacts` / `Engagement Associations` table.

SELECT
    "Engagement Id"                                         AS engagement_id,
    LOWER("Engagement Type")                                AS engagement_type,
    "Engagement Timestamp"                                  AS engaged_at,
    "Owner"                                                 AS engagement_owner_id,
    LOWER("Email Direction")                                AS email_direction,
    LOWER("Email From Address")                             AS email_from_address,
    LOWER(SUBSTRING_INDEX("Email From Address", '@', -1))   AS email_from_domain,
    "Email Subject"                                         AS email_subject
FROM "Engagements"
