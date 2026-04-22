import { sql, MART_SCHEMA } from "./db";

// Helper: build a fully-qualified table reference. We inline MART_SCHEMA as a
// literal because neon's tagged-template driver does not parameterize identifiers.
function t(name: string): string {
  // Reject anything that's not a simple identifier — defense against accidental
  // injection if MART_SCHEMA is ever read from untrusted input.
  const clean = MART_SCHEMA.replace(/[^a-zA-Z0-9_]/g, "");
  return `${clean}.${name}`;
}

export type PartnerRanking = {
  partner_id: string;
  partner_name: string;
  total_leads: number;
  deals_closed_won: number;
  revenue_closed_won: number;
  deal_to_won_rate: number | null;
  lead_to_won_rate: number | null;
  revenue_per_lead: number | null;
  mql_rate: number | null;
  volume_efficiency_quadrant: string;
  rank_by_revenue: number;
  is_top_performer: boolean;
  is_high_potential: boolean;
  is_underperformer: boolean;
  partner_penetration_rate: number | null;
};

export async function getPartnerRankings(): Promise<PartnerRanking[]> {
  const rows = await sql(
    `select partner_id, partner_name, total_leads, deals_closed_won,
            revenue_closed_won, deal_to_won_rate, lead_to_won_rate,
            revenue_per_lead, mql_rate, volume_efficiency_quadrant,
            rank_by_revenue, is_top_performer, is_high_potential,
            is_underperformer, partner_penetration_rate
     from ${t("partner_rankings")}
     order by rank_by_revenue asc`,
  );
  return rows as PartnerRanking[];
}

export type AllTimeSummary = {
  partner_id: string;
  partner_name: string;
  total_leads: number;
  leads_reached_mql: number;
  leads_reached_sql: number;
  total_deals_created: number;
  deals_closed_won: number;
  revenue_closed_won: number;
  partner_penetration_rate: number | null;
};

export async function getAllTimeTotals(): Promise<{
  total_leads: number;
  total_mql: number;
  total_sql: number;
  deals_won: number;
  revenue: number;
  partners_active: number;
}> {
  const rows = await sql(
    `select
        sum(total_leads)::bigint       as total_leads,
        sum(leads_reached_mql)::bigint as total_mql,
        sum(leads_reached_sql)::bigint as total_sql,
        sum(deals_closed_won)::bigint  as deals_won,
        sum(revenue_closed_won)::numeric as revenue,
        count(distinct partner_id)::int  as partners_active
     from ${t("partner_summary")}
     where period_type = 'all_time'`,
  );
  const r = rows[0] || {};
  return {
    total_leads: Number(r.total_leads || 0),
    total_mql: Number(r.total_mql || 0),
    total_sql: Number(r.total_sql || 0),
    deals_won: Number(r.deals_won || 0),
    revenue: Number(r.revenue || 0),
    partners_active: Number(r.partners_active || 0),
  };
}

export type MonthlySummaryPoint = {
  period_start_date: string;
  total_leads: number;
  deals_closed_won: number;
  revenue_closed_won: number;
};
export async function getMonthlySummary(
  partnerId?: string,
): Promise<MonthlySummaryPoint[]> {
  const rows = partnerId
    ? await sql(
        `select period_start_date, total_leads, deals_closed_won, revenue_closed_won
         from ${t("partner_summary")}
         where period_type = 'month' and partner_id = $1
         order by period_start_date asc`,
        [partnerId],
      )
    : await sql(
        `select period_start_date,
                sum(total_leads)::bigint            as total_leads,
                sum(deals_closed_won)::bigint       as deals_closed_won,
                sum(revenue_closed_won)::numeric    as revenue_closed_won
         from ${t("partner_summary")}
         where period_type = 'month'
         group by 1 order by 1 asc`,
      );
  return rows.map((r) => ({
    period_start_date: String(r.period_start_date),
    total_leads: Number(r.total_leads || 0),
    deals_closed_won: Number(r.deals_closed_won || 0),
    revenue_closed_won: Number(r.revenue_closed_won || 0),
  }));
}

export type PartnerSummaryRow = {
  partner_id: string;
  partner_name: string;
  period_type: string;
  period_start_date: string | null;
  total_leads: number;
  leads_reached_mql: number;
  leads_reached_sql: number;
  deals_closed_won: number;
  revenue_closed_won: number;
  mql_rate: number | null;
  sql_rate: number | null;
  deal_to_won_rate: number | null;
  lead_to_won_rate: number | null;
  avg_deal_cycle_days: number | null;
  partner_penetration_rate: number | null;
};

export async function getPartnerAllTime(partnerId: string): Promise<PartnerSummaryRow | null> {
  const rows = await sql(
    `select partner_id, partner_name, period_type, period_start_date,
            total_leads, leads_reached_mql, leads_reached_sql,
            deals_closed_won, revenue_closed_won,
            mql_rate, sql_rate, deal_to_won_rate, lead_to_won_rate,
            avg_deal_cycle_days, partner_penetration_rate
     from ${t("partner_summary")}
     where period_type = 'all_time' and partner_id = $1 limit 1`,
    [partnerId],
  );
  return (rows[0] as PartnerSummaryRow) || null;
}

export async function getFunnelForPartner(partnerId: string) {
  return await sql(
    `select deal_stage, stage_order, deals_entered, deals_advanced,
            deals_closed_won, stage_conversion_rate, stage_to_won_rate,
            next_stage_conversion_rate, drop_off_rate
     from ${t("partner_funnel_stage_conversion")}
     where partner_id = $1 order by stage_order asc`,
    [partnerId],
  );
}

export async function getRepPerfForPartner(partnerId: string) {
  return await sql(
    `select deal_owner_name, deal_owner_email, deals_total, deals_won,
            deals_lost, deals_open, win_rate, revenue_closed_won,
            avg_deal_size, avg_cycle_days
     from ${t("partner_rep_performance")}
     where partner_id = $1
     order by revenue_closed_won desc nulls last`,
    [partnerId],
  );
}

export type PartnerDealRow = {
  deal_id: string;
  deal_name: string | null;
  deal_stage: string | null;
  deal_status: string | null;
  amount: number | null;
  deal_owner_name: string | null;
  deal_owner_email: string | null;
  deal_created_at: string | null;
  deal_close_date: string | null;
  deal_closed_won_at: string | null;
  is_closed_won: boolean;
  is_closed: boolean;
  time_to_close_days: number | null;
  sales_touches_total: number | null;
};

export async function getDealsForPartner(partnerId: string): Promise<PartnerDealRow[]> {
  const rows = await sql(
    `select deal_id, deal_name, deal_stage, deal_status, amount,
            deal_owner_name, deal_owner_email,
            deal_created_at, deal_close_date, deal_closed_won_at,
            is_closed_won, is_closed,
            time_to_close_days, sales_touches_total
     from ${t("partner_deals")}
     where partner_id = $1
     order by
       case when is_closed_won then 0 when not is_closed then 1 else 2 end,
       coalesce(deal_closed_won_at, deal_close_date, deal_created_at) desc nulls last`,
    [partnerId],
  );
  return rows as PartnerDealRow[];
}

export async function getPartnerList(): Promise<{ partner_id: string; partner_name: string }[]> {
  const rows = await sql(
    `select distinct partner_id, partner_name
     from ${t("partner_summary")}
     where period_type = 'all_time'
     order by partner_name asc`,
  );
  return rows as { partner_id: string; partner_name: string }[];
}
