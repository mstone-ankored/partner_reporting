-- Query Table name: partner_rankings
-- Depends on: partner_summary
--
-- Partner rankings + flags, computed off the all_time row so they reflect
-- cumulative performance. Cutpoints for the volume/efficiency quadrant are
-- the median across partners (computed via row-number trick below — Zoho's
-- Query Table SQL does not always expose PERCENTILE_CONT).

WITH base AS (
    SELECT
        partner_id,
        partner_name,
        total_leads,
        total_deals_created,
        deals_closed_won,
        revenue_closed_won,
        deal_to_won_rate,
        lead_to_won_rate,
        revenue_per_lead,
        avg_deal_size,
        mql_rate,
        sql_rate,
        partner_penetration_rate
    FROM "partner_summary"
    WHERE period_type = 'all_time'
),

-- Row-number median: the middle row's value. Works for any warehouse.
ranked_leads AS (
    SELECT total_leads,
           ROW_NUMBER() OVER (ORDER BY total_leads)       AS rn,
           COUNT(*) OVER ()                                AS cnt
    FROM base
),
ranked_rate AS (
    SELECT lead_to_won_rate,
           ROW_NUMBER() OVER (ORDER BY lead_to_won_rate)  AS rn,
           COUNT(*) OVER ()                                AS cnt
    FROM base
    WHERE lead_to_won_rate IS NOT NULL
),
cutpoints AS (
    SELECT
        (SELECT AVG(total_leads) FROM ranked_leads
          WHERE rn IN (FLOOR((cnt + 1) / 2), FLOOR((cnt + 2) / 2))) AS median_leads,
        (SELECT AVG(lead_to_won_rate) FROM ranked_rate
          WHERE rn IN (FLOOR((cnt + 1) / 2), FLOOR((cnt + 2) / 2))) AS median_lead_to_won_rate
),

ranked AS (
    SELECT
        b.*,
        c.median_leads,
        c.median_lead_to_won_rate,
        RANK() OVER (ORDER BY b.revenue_closed_won DESC)           AS rank_by_revenue,
        RANK() OVER (ORDER BY b.deal_to_won_rate   DESC)           AS rank_by_close_rate,
        RANK() OVER (ORDER BY b.revenue_per_lead   DESC)           AS rank_by_revenue_per_lead,
        RANK() OVER (ORDER BY b.total_leads        DESC)           AS rank_by_lead_volume,
        CASE
            WHEN b.total_leads      >= c.median_leads
             AND b.lead_to_won_rate >= c.median_lead_to_won_rate THEN 'high_volume_high_conversion'
            WHEN b.total_leads      >= c.median_leads
             AND b.lead_to_won_rate <  c.median_lead_to_won_rate THEN 'high_volume_low_conversion'
            WHEN b.total_leads      <  c.median_leads
             AND b.lead_to_won_rate >= c.median_lead_to_won_rate THEN 'low_volume_high_conversion'
            ELSE                                                        'low_volume_low_conversion'
        END                                                              AS volume_efficiency_quadrant
    FROM base b
    CROSS JOIN cutpoints c
)

SELECT
    *,
    CASE
        WHEN rank_by_revenue <= 5
          OR volume_efficiency_quadrant = 'high_volume_high_conversion'
        THEN 1 ELSE 0
    END                                                              AS is_top_performer,
    CASE
        WHEN volume_efficiency_quadrant = 'low_volume_high_conversion'
        THEN 1 ELSE 0
    END                                                              AS is_high_potential,
    CASE
        WHEN total_leads       >= median_leads
         AND lead_to_won_rate  <  median_lead_to_won_rate / 2
        THEN 1 ELSE 0
    END                                                              AS is_underperformer
FROM ranked
