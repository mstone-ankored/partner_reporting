-- Query Table name: stg_deal_stage_history
-- Depends on: Deal Stage History (from HubSpot connector)
--
-- If your HubSpot connector in Zoho does not expose stage history as its own
-- table, you'll need to either (a) enable it in the connector config, or
-- (b) skip the stage-by-stage conversion charts. Without history, dashboards
-- that depend on this table (partner_funnel_stage_conversion,
-- partner_deal_stage_durations) will be empty but everything else works.

SELECT
    "Deal Id"                                               AS deal_id,
    LOWER("Stage")                                          AS deal_stage,
    "Entered Timestamp"                                     AS entered_at,
    "Exited Timestamp"                                      AS exited_at,
    COALESCE("Exited Timestamp", NOW())                     AS effective_exited_at,
    TIMESTAMPDIFF(SECOND,
                  "Entered Timestamp",
                  COALESCE("Exited Timestamp", NOW()))      AS seconds_in_stage,
    TIMESTAMPDIFF(SECOND,
                  "Entered Timestamp",
                  COALESCE("Exited Timestamp", NOW())) / 86400.0
                                                            AS days_in_stage
FROM "Deal Stage History"
WHERE "Entered Timestamp" IS NOT NULL
