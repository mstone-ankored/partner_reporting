-- Query Table name: stg_form_submissions
-- Depends on: Form Submissions + Forms (from HubSpot connector)
--
-- The Zoho HubSpot connector tends to land form field values as individual
-- columns (one per field it has seen). That means the "partner_name" form
-- field will show up as its own column named like "Partner Name" or
-- "Partner_Name" — we pick it up here. Adjust the field names to match what
-- Zoho landed (check Data Sources → Form Submissions).

SELECT
    f."Submission Id"                                       AS submission_id,
    f."Contact Id"                                          AS contact_id,
    f."Form Id"                                             AS form_id,
    frm."Form Name"                                         AS form_name,
    f."Submission Timestamp"                                AS submitted_at,
    f."Page Url"                                            AS submission_page_url,

    -- Partner attribution: coalesce across any of the form fields that may
    -- carry the partner name. Add / remove based on your actual form setup.
    COALESCE(
        NULLIF(TRIM(f."Partner Name"),       ''),
        NULLIF(TRIM(f."Referring Partner"),  ''),
        NULLIF(TRIM(f."Partner Referral"),   '')
    )                                                       AS partner_name_from_form,

    -- Common firmographic fields pulled from form submissions.
    NULLIF(f."Company",            '')                      AS form_company_name,
    NULLIF(f."Company Size",       '')                      AS form_company_size,
    NULLIF(f."Number Of Employees", '')                     AS form_num_employees,
    NULLIF(f."Industry",           '')                      AS form_industry,
    NULLIF(f."Country",            '')                      AS form_country,
    NULLIF(f."Use Case",           '')                      AS form_use_case,

    -- Flag: is this a partner-referral form?
    CASE
        WHEN COALESCE(
                 NULLIF(TRIM(f."Partner Name"),       ''),
                 NULLIF(TRIM(f."Referring Partner"),  ''),
                 NULLIF(TRIM(f."Partner Referral"),   '')
             ) IS NOT NULL
            THEN 1
        WHEN LOWER(frm."Form Name") IN ('partner referral', 'partner introduction')
            THEN 1
        ELSE 0
    END                                                     AS is_partner_referral_form
FROM "Form Submissions" f
LEFT JOIN "Forms" frm
       ON frm."Form Id" = f."Form Id"
