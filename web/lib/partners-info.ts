import { sql, MART_SCHEMA, APP_SCHEMA } from "./db";

export type PartnerWithCustomerCount = {
  partner_id: string;
  partner_name: string;
  partner_domain: string | null;
  partner_tier: string | null;
  partner_start_date: string | null;
  latest_customer_count: number | null;
  latest_as_of_date: string | null;
};

export type CustomerCountEntry = {
  id: string;
  partner_id: string;
  as_of_date: string;
  total_customer_count: number;
  notes: string | null;
  created_at: string;
};

// One row per seed partner, joined with their latest customer-count entry
// (from the app-owned partner_customer_counts table).
export async function listPartnersWithCustomerCounts(): Promise<PartnerWithCustomerCount[]> {
  const mart = MART_SCHEMA.replace(/[^a-zA-Z0-9_]/g, "");
  const app = APP_SCHEMA.replace(/[^a-zA-Z0-9_]/g, "");
  const rows = await sql(
    `with latest as (
       select distinct on (partner_id)
         partner_id, as_of_date, total_customer_count
       from ${app}.partner_customer_counts
       order by partner_id, as_of_date desc
     )
     select
       p.partner_id,
       p.partner_name,
       p.partner_domain,
       p.partner_tier,
       p.partner_start_date::text as partner_start_date,
       l.total_customer_count      as latest_customer_count,
       l.as_of_date::text          as latest_as_of_date
     from ${mart}.stg_ref__partners p
     left join latest l using (partner_id)
     order by p.partner_name`,
  );
  return rows as PartnerWithCustomerCount[];
}

export async function listCustomerCountHistory(partnerId: string): Promise<CustomerCountEntry[]> {
  const app = APP_SCHEMA.replace(/[^a-zA-Z0-9_]/g, "");
  const rows = await sql(
    `select id::text, partner_id, as_of_date::text, total_customer_count,
            notes, created_at::text
     from ${app}.partner_customer_counts
     where partner_id = $1
     order by as_of_date desc`,
    [partnerId],
  );
  return rows as CustomerCountEntry[];
}
