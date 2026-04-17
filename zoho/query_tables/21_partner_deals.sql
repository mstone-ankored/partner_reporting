-- Query Table name: partner_deals
-- Depends on: stg_deals, int_deal_primary_contact,
--             int_partner_contact_attribution, ref_partners, stg_owners,
--             int_deal_sales_touches, stg_contacts
--
-- One row per partner-attributed deal. Multiple deals per contact are preserved.

WITH partners AS (
    SELECT
        partner_id,
        partner_name,
        LOWER(TRIM(partner_name)) AS partner_name_key
    FROM "ref_partners"
),

-- Combine: prefer contact-attribution (fidelity) over deal-level custom property.
deal_partner AS (
    SELECT
        d.deal_id,
        COALESCE(a.partner_id,   p_deal.partner_id)                     AS partner_id,
        COALESCE(a.partner_name, d.referring_partner_name_deal)         AS partner_name,
        a.source_type,
        CASE
            WHEN a.partner_name IS NOT NULL                    THEN 'contact_attribution'
            WHEN d.referring_partner_name_deal IS NOT NULL     THEN 'deal_property'
            ELSE NULL
        END                                                              AS partner_attribution_origin
    FROM "stg_deals"                        d
    LEFT JOIN "int_deal_primary_contact"    pc     ON pc.deal_id    = d.deal_id
    LEFT JOIN "int_partner_contact_attribution" a  ON a.contact_id  = pc.primary_contact_id
    LEFT JOIN partners                      p_deal ON p_deal.partner_name_key
                                                   = LOWER(TRIM(d.referring_partner_name_deal))
)

SELECT
    d.deal_id,
    pc.primary_contact_id                                        AS contact_id,
    dp.partner_id,
    dp.partner_name,
    dp.source_type,
    dp.partner_attribution_origin,

    d.deal_name,
    d.pipeline_id,
    d.deal_stage,
    d.deal_type,

    d.amount,
    d.amount_home_currency,

    d.deal_created_at,
    DATE(d.deal_created_at)                                      AS deal_created_date,
    DATE_FORMAT(d.deal_created_at, '%Y-%m-01')                   AS deal_created_month,
    CONCAT(YEAR(d.deal_created_at), '-Q', QUARTER(d.deal_created_at)) AS deal_created_quarter,
    d.deal_close_date,
    DATE(d.deal_close_date)                                      AS deal_close_date_d,
    DATE_FORMAT(d.deal_close_date, '%Y-%m-01')                   AS deal_close_month,
    CONCAT(YEAR(d.deal_close_date), '-Q', QUARTER(d.deal_close_date)) AS deal_close_quarter,
    d.deal_closed_won_at,
    c.contact_created_at,

    d.is_closed_won,
    d.is_closed,
    d.deal_status,

    TIMESTAMPDIFF(SECOND, c.contact_created_at, d.deal_created_at) / 86400.0
                                                                 AS days_contact_to_deal,
    TIMESTAMPDIFF(SECOND, d.deal_created_at,
                  COALESCE(d.deal_closed_won_at, d.deal_close_date, NOW())) / 86400.0
                                                                 AS time_to_close_days,

    st.total_touches                                             AS sales_touches_total,
    st.outbound_emails                                           AS sales_outbound_emails,
    st.calls                                                     AS sales_calls,
    st.meetings                                                  AS sales_meetings,

    d.deal_owner_id,
    o.owner_name                                                 AS deal_owner_name,
    o.owner_email                                                AS deal_owner_email,
    o.owner_team_id                                              AS deal_owner_team_id

FROM "stg_deals"                   d
LEFT JOIN "int_deal_primary_contact" pc ON pc.deal_id   = d.deal_id
LEFT JOIN deal_partner             dp   ON dp.deal_id   = d.deal_id
LEFT JOIN "stg_contacts"           c    ON c.contact_id = pc.primary_contact_id
LEFT JOIN "int_deal_sales_touches" st   ON st.deal_id   = d.deal_id
LEFT JOIN "stg_owners"             o    ON o.owner_id   = d.deal_owner_id
WHERE dp.partner_name IS NOT NULL
