import { neon, neonConfig, Pool } from "@neondatabase/serverless";

neonConfig.fetchConnectionCache = true;

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL is required");
}

// HTTP-based client — use for stateless read queries from React Server
// Components. This gives us automatic connection reuse on Vercel serverless.
export const sql = neon(process.env.DATABASE_URL);

// Pool for transactional writes (auth, notion config). Kept small because
// Vercel functions are short-lived.
let _pool: Pool | undefined;
export function pool(): Pool {
  if (!_pool) {
    _pool = new Pool({ connectionString: process.env.DATABASE_URL });
  }
  return _pool;
}

// Schema qualifier — set to the dbt target schema for marts.
export const MART_SCHEMA = process.env.MART_SCHEMA || "partner_reporting";
// App schema holds auth + notion config, kept separate from marts so dbt
// full-refreshes don't blow them away.
export const APP_SCHEMA = process.env.APP_SCHEMA || "partner_reporting_app";
