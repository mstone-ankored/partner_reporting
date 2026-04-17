-- Query Table name: int_deal_stage_durations
-- Depends on: stg_deal_stage_history, stg_deals
--
-- Total time each deal spent in each stage (summed across re-entries),
-- plus time_to_close for the deal as a whole.

WITH per_stage AS (
    SELECT
        deal_id,
        deal_stage,
        COUNT(*)                               AS times_entered,
        SUM(seconds_in_stage)                  AS total_seconds_in_stage,
        SUM(seconds_in_stage) / 86400.0        AS total_days_in_stage,
        MIN(entered_at)                        AS first_entered_at,
        MAX(effective_exited_at)               AS last_exited_at
    FROM "stg_deal_stage_history"
    GROUP BY deal_id, deal_stage
),
deal_totals AS (
    SELECT
        deal_id,
        TIMESTAMPDIFF(SECOND,
                      deal_created_at,
                      COALESCE(deal_closed_won_at, deal_close_date, NOW()))         AS seconds_to_close,
        TIMESTAMPDIFF(SECOND,
                      deal_created_at,
                      COALESCE(deal_closed_won_at, deal_close_date, NOW())) / 86400.0 AS days_to_close
    FROM "stg_deals"
)

SELECT
    ps.deal_id,
    ps.deal_stage,
    ps.times_entered,
    ps.total_seconds_in_stage,
    ps.total_days_in_stage,
    ps.first_entered_at,
    ps.last_exited_at,
    dt.seconds_to_close,
    dt.days_to_close
FROM per_stage ps
LEFT JOIN deal_totals dt ON dt.deal_id = ps.deal_id
