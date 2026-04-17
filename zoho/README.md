# Zoho-native setup (no warehouse, no dbt)

This is the **recommended path** if you already have Zoho Analytics and want to
avoid standing up a data warehouse. Zoho's native HubSpot connector does the
raw extraction; all the transformation logic lives as Zoho Query Tables (SQL
files in this folder, in dependency order).

```
HubSpot
   │  Zoho Analytics native connector
   ▼
Raw connector tables  ─── seeds (ref_partners, partner_total_customers, uploaded CSVs)
   │
   ▼
Query Tables (the SQL in this folder, built in order)
   ├─ 01–08  stg_*           (renames / normalizes connector columns)
   ├─ 10–14  int_*           (attribution, first-touch, stage durations)
   └─ 20–27  partner_*       (marts — what the dashboards read)
   │
   ▼
Dashboards & reports (see ../docs/dashboard_setup_zoho.md)
```

## One-time setup

### 1. Connect HubSpot to Zoho Analytics

1. In Zoho Analytics → **Create → Connect Data Source → HubSpot**.
2. Authorize with a HubSpot admin account (read-only scopes are fine).
3. On the table picker, select at minimum:
   - Contacts
   - Deals
   - Companies
   - Engagements *(and the Engagement Contacts association)*
   - Deal Contacts *(association table)*
   - Deal Stage History *(enable if available; optional — see note in `07_stg_deal_stage_history.sql`)*
   - Owners
   - Forms
   - Form Submissions
4. Set sync frequency: **15 min** (smallest on most plans; anything below that
   requires an enterprise SKU).

### 2. Upload the two seed CSVs

Both files live in `../seeds/`. In Zoho Analytics:

1. **Create → Import New Table → File**.
2. Upload `seeds/ref_partners.csv`. Name the table exactly `ref_partners`.
3. Upload `seeds/partner_total_customers.csv`. Name it exactly
   `partner_total_customers`.

Whenever you want to update these (onboard a new partner, refresh customer
counts), edit the CSV and use **Data Sources → ref_partners → Import More Data
→ Replace**.

### 3. Create the Query Tables

For each `.sql` file in this folder, in numeric order:

1. **Create → New Query Table**.
2. Set the **Query Table Name** to the name in the comment at the top of the
   file (e.g. `stg_contacts`, `int_partner_contact_attribution`, …).
3. Paste the SQL body.
4. **Save**. Zoho materializes it and caches.

The numeric prefixes enforce build order — do not skip ahead. Every table
only references tables with a lower number.

> **Tip**: if you prefer to script this, Zoho Analytics has a Create-View REST
> endpoint you can call with the SQL body. See `../scripts/bootstrap_zoho_workspace.py`
> for the HTTP plumbing; swap the view type from `TABLE` to `QUERY` and pass
> `sqlQuery` as the body. Requires the `ZohoAnalytics.modeling.all` scope.

### 4. Build the dashboards

Follow `../docs/dashboard_setup_zoho.md`. Every chart binds to one of the
`partner_*` mart tables, which are now live in your workspace.

## Refreshing

- **HubSpot connector** refreshes on its own every 15 min.
- **Query Tables** rebuild automatically when any upstream source changes.
- **Seeds** only change when you re-upload the CSV.

No scheduled jobs, no CI/CD — the whole thing is live.

## When to graduate to the dbt path

Switch to the warehouse-based dbt project (files at the repo root) if any of
the following become true:

- Your HubSpot instance exceeds ~500k contacts or ~100k deals (Zoho Query
  Tables start to slow down).
- You need row-level or per-partner external sharing with strict RLS.
- You want the transformation logic under code review / testing (dbt tests).
- You want to blend HubSpot with other systems (Salesforce, Stripe) that
  Zoho doesn't natively connect to.

The dbt project produces mart tables with identical names, columns, and
semantics — the Zoho dashboards keep working after the swap; only the data
source behind each mart table changes.

## Troubleshooting

| Symptom                                                        | Fix                                                          |
|----------------------------------------------------------------|--------------------------------------------------------------|
| `Unknown column "Contact Id"` when saving a Query Table        | Your connector landed with a different label. Open the Data Source and check the exact column name, then update the stg_* SQL. |
| `partner_leads` returns 0 rows but `stg_contacts` has many     | Partner attribution rules didn't match anyone. Check `int_partner_contact_attribution` on its own; then verify `ref_partners.csv` has the domains/names you're seeing in HubSpot. |
| `partner_funnel_stage_conversion` is empty                     | Your connector didn't pull Deal Stage History. Re-configure the connector to include it. |
| `median_leads` in `partner_rankings` is NULL                   | Not enough partners yet (need ≥ 2). Hide the quadrant chart until you have more data. |
| Funnel stage names don't match                                 | Edit the `stage_order` CTE in `22_partner_funnel_stage_conversion.sql` to match your HubSpot pipeline. |
