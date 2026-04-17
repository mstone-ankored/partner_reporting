-- Query Table name: stg_deals
-- Depends on: Deals (from HubSpot connector)

SELECT
    "Deal Id"                                               AS deal_id,
    "Deal Name"                                             AS deal_name,
    "Pipeline"                                              AS pipeline_id,
    LOWER("Deal Stage")                                     AS deal_stage,
    CAST("Amount" AS DECIMAL)                               AS amount,
    CAST("Amount In Company Currency" AS DECIMAL)           AS amount_home_currency,
    "Create Date"                                           AS deal_created_at,
    "Close Date"                                            AS deal_close_date,
    "Date Entered Closed Won"                               AS deal_closed_won_at,
    CASE
        WHEN "Is Deal Closed Won" = 'true' OR "Is Deal Closed Won" = '1' THEN 1
        ELSE 0
    END                                                     AS is_closed_won,
    CASE
        WHEN "Is Deal Closed"     = 'true' OR "Is Deal Closed"     = '1' THEN 1
        ELSE 0
    END                                                     AS is_closed,
    CASE
        WHEN "Is Deal Closed Won" = 'true' THEN 'won'
        WHEN "Is Deal Closed"     = 'true' THEN 'lost'
        ELSE 'open'
    END                                                     AS deal_status,
    "Deal Type"                                             AS deal_type,
    "Deal Owner"                                            AS deal_owner_id,
    NULLIF(TRIM("Referring Partner"), '')                   AS referring_partner_name_deal
FROM "Deals"
