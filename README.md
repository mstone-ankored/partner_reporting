# Partner Performance Reporting

Production-ready analytics stack that turns raw HubSpot data (contacts, deals,
engagements, form submissions) into partner-level performance tables, a live
interactive web dashboard, and a configurable push of rolled-up metrics into
Notion.

## Stack

- **Warehouse** — [Neon](https://neon.tech) (serverless Postgres), provisioned
  through the Vercel Neon integration.
- **Transformations** — dbt (`dbt-postgres`) — staging → intermediate → marts.
- **Dashboard** — Next.js app in [`web/`](web/), deployed to Vercel. Reads
  directly from Neon. Handles auth, drilldowns, forecasting, and Notion sync
  configuration.
- **Notion push** — `scripts/notion_sync.py` reads a per-table config saved by
  the web app and upserts selected rows into Notion databases.

## Pipeline at a glance

```
HubSpot (raw, landed by Fivetran/Airbyte into Neon)
        │
        ▼
staging/    ── stg_hubspot__contacts, stg_hubspot__deals, …
        │
        ▼
intermediate/ ── int_partner_contact_attribution, int_deal_stage_durations, …
        │
        ▼
marts/      ── partner_leads, partner_deals, partner_summary,
               partner_funnel_stage_conversion, partner_deal_stage_durations,
               partner_rep_performance, partner_rankings, partner_penetration,
               partner_lead_cohorts
        │
        ├──▶ Next.js web app on Vercel  (interactive dashboard + forecasts)
        └──▶ scripts/notion_sync.py     (selected rows → Notion databases)
```

## What the dashboard can answer

| Question                                            | Primary table                              |
|-----------------------------------------------------|--------------------------------------------|
| Which partners drive the most revenue?              | `partner_rankings`, `partner_summary`      |
| Which partners drive the highest-quality leads?     | `partner_summary` (mql_rate, sql_rate)     |
| Where do partner leads break in the funnel?         | `partner_funnel_stage_conversion`          |
| Which partners should we invest in vs deprioritize? | `partner_rankings.volume_efficiency_quadrant` |
| How does a partner's revenue trend?                 | `partner_summary` (period_type = 'month')  |
| What should next quarter look like per partner?     | forecast page (built on `partner_summary`) |
| Which reps close partner deals best?                | `partner_rep_performance`                  |
| How deeply have we penetrated a partner's book?     | `partner_penetration`                      |

## Quickstart

1. **Provision Neon via Vercel.** See [`docs/vercel_neon_setup.md`](docs/vercel_neon_setup.md).
2. **Build the marts.**
   ```bash
   make install                                  # dbt + Python deps
   cp profiles.yml.example ~/.dbt/profiles.yml   # fill in Neon creds
   make build                                    # seed + run + test
   ```
3. **Run the web app locally.**
   ```bash
   cd web && npm install && npm run dev
   ```
   Full walkthrough: [`docs/web_app_setup.md`](docs/web_app_setup.md).
4. **Wire up Notion (optional).** See [`docs/notion_setup.md`](docs/notion_setup.md).

## Scheduled refresh

`.github/workflows/refresh.yml` runs every 30 min:
1. `dbt build` against Neon.
2. `python scripts/notion_sync.py` — pushes rows selected in the web app's
   Notion sync settings into the configured Notion databases.

The web app queries Neon live, so the dashboard is always fresh once dbt
finishes; Notion is eventual-consistency on the 30-min cadence.

## Adding or updating data

- **New partner**: add a row to `seeds/ref_partners.csv` → `dbt seed`.
- **New partner customer counts**: append rows to `seeds/partner_total_customers.csv`
  with a fresh `as_of_date` → `dbt seed` → `partner_penetration` picks it up.
- **Different HubSpot property names**: override `vars:` in `dbt_project.yml`
  or via `dbt build --vars '{partner_name_contact_property: my_custom_field}'`.

## Documentation

- [`docs/data_model.md`](docs/data_model.md) — ER diagram, column reference, join keys.
- [`docs/assumptions.md`](docs/assumptions.md) — business rules encoded in the models.
- [`docs/dashboard_structure.md`](docs/dashboard_structure.md) — dashboard pages and the tables each chart binds to.
- [`docs/metrics_glossary.md`](docs/metrics_glossary.md) — exact metric formulas.
- [`docs/vercel_neon_setup.md`](docs/vercel_neon_setup.md) — provision Neon via Vercel, deploy the app.
- [`docs/web_app_setup.md`](docs/web_app_setup.md) — local dev for the Next.js app.
- [`docs/notion_setup.md`](docs/notion_setup.md) — create Notion integration + databases, configure sync.
