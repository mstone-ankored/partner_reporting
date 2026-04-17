-- Query Table name: int_contact_first_touch
-- Depends on: stg_engagements, stg_engagement_contacts
--
-- Earliest engagement + earliest sales touch per contact. Used to compute
-- "time to first sales touch".

WITH joined AS (
    SELECT
        ec.contact_id,
        e.engagement_type,
        e.engaged_at,
        e.email_direction
    FROM "stg_engagements"         e
    JOIN "stg_engagement_contacts" ec ON ec.engagement_id = e.engagement_id
)

SELECT
    contact_id,
    MIN(engaged_at) AS first_touch_at,
    MIN(
        CASE
            WHEN engagement_type IN ('call', 'meeting')
              OR (engagement_type = 'email' AND email_direction = 'outgoing')
            THEN engaged_at
        END
    ) AS first_sales_touch_at
FROM joined
GROUP BY contact_id
