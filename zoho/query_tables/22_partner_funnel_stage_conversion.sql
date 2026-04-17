-- Query Table name: partner_funnel_stage_conversion
-- Depends on: partner_deals, int_deal_stage_durations
--
-- Per-partner per-stage conversion and drop-off. Stage order is hardcoded
-- here; if your pipeline uses different stages, edit the CASE WHEN.

WITH stage_order AS (
    SELECT 'discovery'   AS deal_stage, 1 AS stage_order
    UNION SELECT 'demo',         2
    UNION SELECT 'proposal',     3
    UNION SELECT 'negotiation',  4
    UNION SELECT 'closed_won',   5
),

-- Every (deal, stage) the deal entered, with its numeric order.
entries AS (
    SELECT
        pd.partner_id,
        pd.partner_name,
        pd.deal_id,
        sd.deal_stage,
        so.stage_order,
        pd.is_closed_won
    FROM "partner_deals"              pd
    JOIN "int_deal_stage_durations"   sd ON sd.deal_id    = pd.deal_id
    JOIN stage_order                  so ON so.deal_stage = sd.deal_stage
),

-- Furthest stage order each deal reached (for forward-advancement logic).
max_stage AS (
    SELECT
        partner_id,
        deal_id,
        MAX(stage_order) AS max_stage_order,
        MAX(is_closed_won) AS is_closed_won
    FROM entries
    GROUP BY partner_id, deal_id
),

per_stage AS (
    SELECT
        e.partner_id,
        e.partner_name,
        e.deal_stage,
        e.stage_order,
        COUNT(DISTINCT e.deal_id)                                              AS deals_entered,
        COUNT(DISTINCT CASE WHEN m.max_stage_order >  e.stage_order THEN e.deal_id END) AS deals_advanced,
        COUNT(DISTINCT CASE WHEN m.max_stage_order >= e.stage_order + 1 THEN e.deal_id END) AS deals_entered_next_stage,
        COUNT(DISTINCT CASE WHEN m.is_closed_won = 1 THEN e.deal_id END)       AS deals_closed_won
    FROM entries    e
    JOIN max_stage  m ON m.partner_id = e.partner_id AND m.deal_id = e.deal_id
    GROUP BY e.partner_id, e.partner_name, e.deal_stage, e.stage_order
)

SELECT
    partner_id,
    partner_name,
    deal_stage,
    stage_order,
    deals_entered,
    deals_advanced,
    deals_closed_won,
    CASE WHEN deals_entered > 0
         THEN deals_advanced * 1.0 / deals_entered END               AS stage_conversion_rate,
    CASE WHEN deals_entered > 0
         THEN deals_closed_won * 1.0 / deals_entered END             AS stage_to_won_rate,
    deals_entered_next_stage,
    CASE WHEN deals_entered > 0
         THEN deals_entered_next_stage * 1.0 / deals_entered END     AS next_stage_conversion_rate,
    CASE WHEN deals_entered > 0
         THEN 1 - (deals_entered_next_stage * 1.0 / deals_entered) END AS drop_off_rate
FROM per_stage
