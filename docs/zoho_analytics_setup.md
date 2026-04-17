# Connecting partner_reporting to Zoho Analytics

There are two supported paths. Pick the one that matches your infrastructure;
both end at the same place (live Zoho Analytics dashboards off the mart
tables). **Path A is simpler** if you have a warehouse Zoho supports natively.

```
HubSpot ──▶ Fivetran/Airbyte ──▶ Warehouse ──▶ dbt build ──▶ Mart tables
                                                                 │
                    ┌────────────────────────────────────────────┤
                    │                                            │
            Path A: native                             Path B: API push
            Zoho live connector                        (scripts/zoho_sync.py)
                    │                                            │
                    └────────────────────────┬───────────────────┘
                                             ▼
                                     Zoho Analytics Workspace
                                     (dashboards read from here)
```

---

## Path A — Zoho native connector (recommended if you use BigQuery, Snowflake, Redshift, or Postgres)

Zoho Analytics has a managed connector for these warehouses that syncs every
15 / 30 / 60 min without writing any code.

### 1. Build the mart tables once

```bash
make install
make build        # populates partner_reporting_marts schema in your warehouse
```

### 2. In Zoho Analytics

1. Create a **Workspace** (or open an existing one) → **Create Table / Data
   Source** → choose your warehouse (BigQuery / Snowflake / Redshift /
   Postgres).
2. Provide read-only credentials for a service account scoped to
   `partner_reporting_marts`. Do **not** use an admin account.
3. On the table picker screen, select all 9 mart tables:
   - `partner_leads`
   - `partner_deals`
   - `partner_summary`
   - `partner_funnel_stage_conversion`
   - `partner_deal_stage_durations`
   - `partner_rep_performance`
   - `partner_rankings`
   - `partner_penetration`
   - `partner_lead_cohorts`
4. Set the sync mode:
   - **Live Connect** (BigQuery / Snowflake only) — queries pass through to
     the warehouse at report time. Sub-minute freshness; no sync cost.
   - **Fetch & Store** — Zoho pulls rows on a schedule. Set interval to 15 min
     for the closest thing to real-time on plans that allow it.

### 3. Schedule dbt

Use the GitHub Actions workflow in `.github/workflows/refresh.yml` (runs every
30 min) or your own orchestrator. When dbt finishes, Zoho sees fresh rows on
the next tick of its sync.

**Freshness envelope**: dbt cadence + Zoho sync cadence. 30 min dbt + 15 min
Zoho = worst-case 45 min lag.

---

## Path B — API push (via `scripts/zoho_sync.py`)

Use this when:
- Your warehouse isn't in Zoho's native list, or
- You can't open warehouse credentials to Zoho, or
- You want sub-15-min freshness on plans that don't offer it natively.

The script reads each mart table over SQLAlchemy and streams it into Zoho's
Bulk Import API.

### 1. Create a Zoho self-client and get OAuth credentials

1. Go to the [Zoho API Console](https://api-console.zoho.com/) for your
   region (`.com`, `.eu`, `.in`, `.com.au`, `.jp`).
2. Click **Add Client** → **Self Client** → **Create**.
3. Copy the **Client ID** and **Client Secret**.
4. Under the **Generate Code** tab, request these scopes (paste as a
   comma-separated list):

   ```
   ZohoAnalytics.fullaccess.all,ZohoAnalytics.data.all,ZohoAnalytics.import.all,ZohoAnalytics.metadata.all
   ```

5. Set an expiry of 10 minutes and click **Create**. You get a **grant token**.
6. Exchange it for a **refresh token** (one-time, never expires until revoked):

   ```bash
   curl -X POST 'https://accounts.zoho.com/oauth/v2/token' \
        -d 'grant_type=authorization_code' \
        -d 'client_id=YOUR_CLIENT_ID' \
        -d 'client_secret=YOUR_CLIENT_SECRET' \
        -d 'code=YOUR_GRANT_TOKEN' \
        -d 'redirect_uri=https://www.zoho.com'
   ```

   Save the `refresh_token` from the response; it is the only one you need going forward.

### 2. Find your Org and Workspace IDs

- **Org ID**: log in to Zoho Analytics → **Settings** → **Organization
  Details** → look for `Org ID`.
- **Workspace ID**: open the target workspace, look at the URL:
  `…/workspace/<WORKSPACE_ID>/…`.

### 3. Set env vars

```bash
export WAREHOUSE_URL='bigquery://my-project/partner_reporting_marts'
export WAREHOUSE_MART_SCHEMA='partner_reporting_marts'

export ZOHO_REGION='com'
export ZOHO_CLIENT_ID='1000.XXXXXXXXXX'
export ZOHO_CLIENT_SECRET='YYYYYYYYYY'
export ZOHO_REFRESH_TOKEN='1000.ZZZZZZZZZZ'
export ZANALYTICS_ORGID='123456789'
export ZOHO_WORKSPACE_ID='987654321'
```

### 4. One-time: create the Zoho tables

```bash
make build          # make sure warehouse tables exist so we can reflect schema
make bootstrap      # creates 9 views in Zoho matching your mart schema
```

### 5. Sync

```bash
make sync
```

Expected output:

```
INFO Acquired Zoho access token for region com
INFO Zoho workspace 987654321 has 9 views
INFO Imported partner_penetration via TRUNCATEADD (job=abc…)
INFO Imported partner_rankings via TRUNCATEADD (job=def…)
…
INFO Imported partner_leads via UPDATEADD (job=…)
INFO Imported partner_deals via UPDATEADD (job=…)
INFO Zoho sync complete
```

### 6. Schedule it

Add the env vars to GitHub Actions secrets and enable
`.github/workflows/refresh.yml`. The workflow runs `dbt build && make sync`
every 30 min.

---

## Upsert vs truncate-add

`zoho_sync.py` uses two import modes (see `TABLES` in the script):

| Mode          | Tables                                    | Semantics                          |
|---------------|-------------------------------------------|-------------------------------------|
| `TRUNCATEADD` | All aggregate / dim tables                | Zoho wipes + replaces. Always correct; size is small. |
| `UPDATEADD`   | `partner_leads`, `partner_deals`          | Upsert on `contact_id` / `deal_id`. Only new & changed rows re-upload. |

For volumes up to ~1M rows, `TRUNCATEADD` on the facts also works and is
simpler. Flip a table to `TRUNCATEADD` by editing the `TABLES` list.

## Troubleshooting

| Symptom                                                  | Likely cause                                                    |
|----------------------------------------------------------|------------------------------------------------------------------|
| `Zoho did not return an access_token`                    | Refresh token revoked, or wrong region. Re-do step 1.           |
| `Zoho view 'partner_leads' does not exist`               | You haven't run `make bootstrap` yet.                           |
| `403 Forbidden` on import                                | Self-client scopes don't include `ZohoAnalytics.import.all`.    |
| Import succeeds but dashboard shows stale numbers        | Zoho's query cache. Refresh the report or lower cache TTL.      |
| `Could not reflect partner_leads`                        | Table does not exist in the warehouse — run `make build` first. |
| Sync runs on schedule but numbers don't change           | dbt isn't running. Check the Actions workflow log.              |

## Scaling notes

- `pull_table_as_csv` reads the whole table into memory. For tables >1M rows,
  switch to chunked reads: paginate via `LIMIT/OFFSET` or a watermark column
  (`_synced_at >= last_sync_at`), and call `import_to_view` per chunk with
  `importType = "APPEND"` on subsequent chunks.
- Zoho's Bulk Import supports up to 2 GB per request.
- For truly huge tables (tens of millions of rows), use Path A + Live Connect
  on BigQuery/Snowflake; the warehouse stays the source of truth and Zoho
  never stores the rows.
