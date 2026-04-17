-- Query Table name: stg_contacts
-- Depends on: Contacts (from HubSpot connector)
--
-- Normalizes HubSpot connector column names to the shorter snake_case names
-- used by everything downstream. If your connector lands columns with
-- different labels, only this file needs editing.

SELECT
    "Contact Id"                                            AS contact_id,
    LOWER("Email")                                          AS email,
    LOWER(SUBSTRING_INDEX("Email", '@', -1))                AS email_domain,
    "First Name"                                            AS first_name,
    "Last Name"                                             AS last_name,
    "Company Name"                                          AS company_name,
    "Job Title"                                             AS job_title,
    "Industry"                                              AS industry,
    CAST("Number Of Employees" AS DECIMAL)                  AS company_size_employees,
    CAST("Annual Revenue"      AS DECIMAL)                  AS company_annual_revenue,
    LOWER("Lifecycle Stage")                                AS lifecycle_stage,
    LOWER("Lead Status")                                    AS lead_status,
    "Create Date"                                           AS contact_created_at,
    "Became A Lead Date"                                    AS became_lead_at,
    "Became A Marketing Qualified Lead Date"                AS became_mql_at,
    "Became A Sales Qualified Lead Date"                    AS became_sql_at,
    "Became An Opportunity Date"                            AS became_opportunity_at,
    "Became A Customer Date"                                AS became_customer_at,
    "Original Source"                                       AS original_source,
    "Original Source Drill Down 1"                          AS original_source_drill_down_1,
    "Original Source Drill Down 2"                          AS original_source_drill_down_2,
    "Latest Source"                                         AS latest_source,
    "Owner"                                                 AS contact_owner_id,
    -- Custom partner fields. Rename to whatever your HubSpot admin named them.
    NULLIF(TRIM("Referring Partner"), '')                   AS referring_partner_name_declared,
    LOWER(NULLIF(TRIM("Partner Source Type"), ''))          AS partner_source_type_declared
FROM "Contacts"
