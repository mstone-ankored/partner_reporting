-- Query Table name: stg_owners
-- Depends on: Owners (from HubSpot connector)

SELECT
    "Owner Id"                                              AS owner_id,
    "Email"                                                 AS owner_email,
    "First Name"                                            AS owner_first_name,
    "Last Name"                                             AS owner_last_name,
    TRIM(CONCAT(COALESCE("First Name", ''), ' ', COALESCE("Last Name", '')))
                                                            AS owner_name,
    "Team Id"                                               AS owner_team_id,
    "Create Date"                                           AS owner_created_at,
    CASE
        WHEN "Archived" = 'true' OR "Archived" = '1' THEN 1
        ELSE 0
    END                                                     AS is_archived
FROM "Owners"
