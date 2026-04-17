-- Query Table name: int_partner_contact_attribution
-- Depends on: stg_contacts, stg_form_submissions, stg_engagements,
--             stg_engagement_contacts, ref_partners
--
-- For every contact that we can link to a partner, emits exactly one row with:
--   * partner_id / partner_name   (canonicalized via ref_partners)
--   * source_type                 ('partner_email' | 'form')
--   * attribution_method          (which rule matched; for audit)
--
-- Priority order (highest-confidence first, first match wins per contact):
--   1. HubSpot custom contact property `referring_partner_name_declared` set
--   2. Partner-referral form submission
--   3. Inbound email engagement from a partner domain (ref_partners)
--   4. HubSpot Original Source = 'REFERRALS' with drill-down matching partner

WITH

-- Canonical partner dimension with a lowercased join key.
partners AS (
    SELECT
        partner_id,
        partner_name,
        LOWER(TRIM(partner_name)) AS partner_name_key,
        LOWER(partner_domain)      AS partner_domain
    FROM "ref_partners"
),

-- ---- METHOD 1: declared HubSpot property -----------------------------------
declared_property AS (
    SELECT
        c.contact_id,
        p.partner_id,
        p.partner_name,
        CASE
            WHEN c.partner_source_type_declared IN ('partner_email', 'email', 'referral_email') THEN 'partner_email'
            WHEN c.partner_source_type_declared IN ('form', 'form_submission')                   THEN 'form'
            ELSE 'partner_email'
        END AS source_type,
        1 AS priority,
        'declared_property' AS attribution_method,
        CAST(NULL AS TIMESTAMP) AS evidence_at
    FROM "stg_contacts" c
    JOIN partners p
      ON LOWER(TRIM(c.referring_partner_name_declared)) = p.partner_name_key
    WHERE c.referring_partner_name_declared IS NOT NULL
),

-- ---- METHOD 2: partner-referral form ---------------------------------------
form_raw AS (
    SELECT
        f.contact_id,
        COALESCE(p.partner_id,    NULL)                      AS partner_id,
        COALESCE(p.partner_name,  f.partner_name_from_form)  AS partner_name,
        'form'                   AS source_type,
        2                        AS priority,
        'form_submission'        AS attribution_method,
        f.submitted_at           AS evidence_at,
        ROW_NUMBER() OVER (
            PARTITION BY f.contact_id
            ORDER BY f.submitted_at ASC, f.submission_id ASC
        ) AS rn
    FROM "stg_form_submissions" f
    LEFT JOIN partners p
      ON LOWER(TRIM(f.partner_name_from_form)) = p.partner_name_key
    WHERE f.is_partner_referral_form = 1
),
form_attribution AS (
    SELECT contact_id, partner_id, partner_name, source_type, priority,
           attribution_method, evidence_at
    FROM form_raw
    WHERE rn = 1 AND partner_name IS NOT NULL
),

-- ---- METHOD 3: inbound email from a partner domain -------------------------
partner_email_raw AS (
    SELECT
        ec.contact_id,
        p.partner_id,
        p.partner_name,
        'partner_email'          AS source_type,
        3                        AS priority,
        'partner_email_domain'   AS attribution_method,
        e.engaged_at             AS evidence_at,
        ROW_NUMBER() OVER (
            PARTITION BY ec.contact_id
            ORDER BY e.engaged_at ASC, e.engagement_id ASC
        ) AS rn
    FROM "stg_engagements"          e
    JOIN "stg_engagement_contacts"  ec ON ec.engagement_id = e.engagement_id
    JOIN partners p
      ON e.email_from_domain = p.partner_domain
    WHERE e.engagement_type = 'email'
      AND e.email_direction = 'incoming'
),
email_attribution AS (
    SELECT contact_id, partner_id, partner_name, source_type, priority,
           attribution_method, evidence_at
    FROM partner_email_raw
    WHERE rn = 1
),

-- ---- METHOD 4: HubSpot original_source = REFERRALS -------------------------
referral_source AS (
    SELECT
        c.contact_id,
        p.partner_id,
        p.partner_name,
        'partner_email'             AS source_type,
        4                           AS priority,
        'referral_original_source'  AS attribution_method,
        CAST(NULL AS TIMESTAMP)     AS evidence_at
    FROM "stg_contacts" c
    JOIN partners p
      ON LOWER(TRIM(c.original_source_drill_down_1)) = p.partner_name_key
      OR LOWER(TRIM(c.original_source_drill_down_2)) = p.partner_name_key
    WHERE UPPER(c.original_source) IN ('REFERRALS', 'REFERRAL')
),

-- Combine and keep the highest-priority match per contact.
unioned AS (
    SELECT * FROM declared_property
    UNION ALL SELECT * FROM form_attribution
    UNION ALL SELECT * FROM email_attribution
    UNION ALL SELECT * FROM referral_source
),
ranked AS (
    SELECT
        u.*,
        ROW_NUMBER() OVER (
            PARTITION BY contact_id
            ORDER BY priority ASC, evidence_at ASC
        ) AS rn
    FROM unioned u
    WHERE partner_name IS NOT NULL
)

SELECT
    contact_id,
    partner_id,
    partner_name,
    source_type,
    attribution_method,
    evidence_at AS attribution_evidence_at
FROM ranked
WHERE rn = 1
