import { pool, APP_SCHEMA } from "./db";

// The set of mart tables users can push to Notion. For each: a display label,
// the natural key (used as the Notion "title" property), and the columns we
// offer for column-mapping. Keeping this list explicit prevents users from
// accidentally trying to sync a 10M-row table to Notion.
export type NotionSourceDef = {
  name: string;
  label: string;
  titleColumn: string;
  columns: { key: string; label: string; kind: "text" | "number" | "percent" | "money" | "date" | "bool" }[];
  filters: { key: string; label: string; values: string[] }[];
  whereHint?: string;
};

export const NOTION_SOURCES: NotionSourceDef[] = [
  {
    name: "partner_rankings",
    label: "Partner rankings",
    titleColumn: "partner_name",
    columns: [
      { key: "partner_name", label: "Partner", kind: "text" },
      { key: "rank_by_revenue", label: "Rank (revenue)", kind: "number" },
      { key: "total_leads", label: "Leads", kind: "number" },
      { key: "deals_closed_won", label: "Deals won", kind: "number" },
      { key: "revenue_closed_won", label: "Revenue", kind: "money" },
      { key: "revenue_per_lead", label: "Revenue / lead", kind: "money" },
      { key: "lead_to_won_rate", label: "Lead → won", kind: "percent" },
      { key: "mql_rate", label: "MQL rate", kind: "percent" },
      { key: "volume_efficiency_quadrant", label: "Quadrant", kind: "text" },
      { key: "is_top_performer", label: "Top performer", kind: "bool" },
      { key: "is_high_potential", label: "High potential", kind: "bool" },
      { key: "is_underperformer", label: "Underperformer", kind: "bool" },
    ],
    filters: [],
  },
  {
    name: "partner_summary",
    label: "Partner summary (all-time only by default)",
    titleColumn: "partner_name",
    columns: [
      { key: "partner_name", label: "Partner", kind: "text" },
      { key: "total_leads", label: "Leads", kind: "number" },
      { key: "leads_reached_mql", label: "MQLs", kind: "number" },
      { key: "deals_closed_won", label: "Deals won", kind: "number" },
      { key: "revenue_closed_won", label: "Revenue", kind: "money" },
      { key: "mql_rate", label: "MQL rate", kind: "percent" },
      { key: "deal_to_won_rate", label: "Deal → won", kind: "percent" },
      { key: "lead_to_won_rate", label: "Lead → won", kind: "percent" },
      { key: "partner_penetration_rate", label: "Penetration", kind: "percent" },
    ],
    filters: [
      { key: "period_type", label: "Period", values: ["all_time", "month", "quarter"] },
    ],
  },
  {
    name: "partner_penetration",
    label: "Partner penetration",
    titleColumn: "partner_name",
    columns: [
      { key: "partner_name", label: "Partner", kind: "text" },
      { key: "as_of_date", label: "As of", kind: "date" },
      { key: "total_customer_count", label: "Partner total customers", kind: "number" },
      { key: "our_customer_count", label: "Our customers", kind: "number" },
      { key: "penetration_rate", label: "Penetration", kind: "percent" },
    ],
    filters: [],
  },
];

export type NotionTargetRow = {
  id: string;
  source_table: string;
  notion_database_id: string;
  filter_json: Record<string, unknown>;
  column_map_json: Record<string, string>;
  enabled: boolean;
  last_synced_at: string | null;
  last_sync_status: string | null;
  last_sync_message: string | null;
};

export async function listTargets(): Promise<NotionTargetRow[]> {
  const { rows } = await pool().query(
    `select id, source_table, notion_database_id, filter_json, column_map_json,
            enabled, last_synced_at, last_sync_status, last_sync_message
     from ${APP_SCHEMA}.notion_sync_targets
     order by source_table asc, created_at asc`,
  );
  return rows as NotionTargetRow[];
}
