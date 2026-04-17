-- Query Table name: partner_leads
-- Depends on: int_partner_contact_attribution, stg_contacts,
--             int_contact_first_touch, stg_form_submissions
--
-- One row per partner-sourced inbound lead. This is the primary dashboard
-- table for lead-volume / lead-quality charts.

WITH form_first AS (
    SELECT
        f.contact_id,
        f.form_id, f.form_name, f.submitted_at,
        f.form_company_name, f.form_company_size, f.form_num_employees,
        f.form_industry, f.form_country, f.form_use_case,
        ROW_NUMBER() OVER (
            PARTITION BY f.contact_id
            ORDER BY f.submitted_at ASC, f.submission_id ASC
        ) AS rn
    FROM "stg_form_submissions" f
    WHERE f.is_partner_referral_form = 1
),
form_meta AS (
    SELECT * FROM form_first WHERE rn = 1
)

SELECT
    c.contact_id,
    a.partner_id,
    a.partner_name,
    a.source_type,
    a.attribution_method,
    a.attribution_evidence_at,

    c.email,
    c.email_domain,
    c.first_name,
    c.last_name,
    c.company_name,
    c.job_title,
    c.industry,
    c.company_size_employees,
    c.company_annual_revenue,
    c.contact_owner_id,

    c.contact_created_at                                       AS lead_created_at,
    DATE(c.contact_created_at)                                 AS lead_created_date,
    DATE_FORMAT(c.contact_created_at, '%Y-%m-01')              AS lead_created_month,
    CONCAT(YEAR(c.contact_created_at), '-Q', QUARTER(c.contact_created_at)) AS lead_created_quarter,
    ft.first_touch_at,
    ft.first_sales_touch_at,
    TIMESTAMPDIFF(HOUR, c.contact_created_at, ft.first_sales_touch_at)
                                                               AS hours_to_first_sales_touch,

    c.lifecycle_stage,
    c.lead_status,
    c.became_lead_at,
    c.became_mql_at,
    c.became_sql_at,
    c.became_opportunity_at,
    c.became_customer_at,
    CASE
        WHEN LOWER(c.lifecycle_stage) IN ('marketingqualifiedlead', 'mql')
          OR c.became_mql_at IS NOT NULL THEN 1 ELSE 0
    END                                                        AS reached_mql,
    CASE
        WHEN LOWER(c.lifecycle_stage) IN ('salesqualifiedlead', 'sql')
          OR c.became_sql_at IS NOT NULL THEN 1 ELSE 0
    END                                                        AS reached_sql,
    CASE
        WHEN LOWER(c.lifecycle_stage) IN ('other', 'disqualified')
          OR LOWER(c.lead_status) IN ('unqualified', 'disqualified', 'bad_fit', 'dq')
        THEN 1 ELSE 0
    END                                                        AS is_disqualified,

    fm.form_id,
    fm.form_name,
    fm.submitted_at                                            AS form_submitted_at,
    fm.form_company_name,
    fm.form_company_size,
    fm.form_num_employees,
    fm.form_industry,
    fm.form_country,
    fm.form_use_case,

    c.original_source,
    c.original_source_drill_down_1,
    c.original_source_drill_down_2

FROM "stg_contacts"                    c
JOIN "int_partner_contact_attribution" a  ON a.contact_id = c.contact_id
LEFT JOIN "int_contact_first_touch"    ft ON ft.contact_id = c.contact_id
LEFT JOIN form_meta                    fm ON fm.contact_id = c.contact_id
