# Partner Performance Reporting

Production-ready analytics stack that turns raw HubSpot data (contacts, deals,
engagements, form submissions) into partner-level performance tables that
power a live Zoho Analytics dashboard.

Two implementations ship in this repo — pick one:

1. **Zoho-native** (recommended, no warehouse) — Zoho Analytics' HubSpot
   connector pulls raw data; the `zoho/query_tables/*.sql` files are Query
   Tables you create in Zoho that layer the same semantics. No code runs
   outside Zoho. See [`zoho/README.md`](zoho/README.md).
2. **dbt + warehouse** — production dbt project (BigQuery / Snowflake /
   Postgres / Redshift) plus a Python sync that pushes mart tables to Zoho.
   Use this when you outgrow Zoho Query Tables or need to blend HubSpot with
   other systems. See [`docs/zoho_analytics_setup.md`](docs/zoho_analytics_setup.md).

Both produce mart tables with identical names, columns, and semantics, so the
dashboard built on one works unchanged on the other.

## Pipeline at a glance

```
HubSpot (raw, landed by Fivetran/Airbyte)
        │
        ▼
staging/   ── stg_hubspot__contacts, stg_hubspot__deals, stg_hubspot__engagements, …
        │
        ▼
intermediate/ ── int_partner_contact_attribution, int_contact_first_touch,
                 int_deal_primary_contact, int_deal_stage_durations,
                 int_deal_sales_touches
        │
        ▼
marts/     ── partner_leads          (1 row per lead)
              partner_deals          (1 row per deal)
              partner_summary        (KPIs by partner × period)
              partner_funnel_stage_conversion
              partner_deal_stage_durations
              partner_rep_performance
              partner_rankings
              partner_penetration
              partner_lead_cohorts
```

## What the dashboard can answer

| Question                                            | Primary table                              |
|-----------------------------------------------------|--------------------------------------------|
| Which partners drive the most revenue?              | `partner_rankings`, `partner_summary`      |
| Which partners drive the highest-quality leads?     | `partner_summary` (mql_rate, sql_rate)     |
| Where do partner leads break in the funnel?         | `partner_funnel_stage_conversion`          |
| Which partners should we invest in vs deprioritize? | `partner_rankings.volume_efficiency_quadrant` |
| How does a partner's revenue trend?                 | `partner_summary` (period_type = 'month')  |
| Which reps close partner deals best?                | `partner_rep_performance`                  |
| How deeply have we penetrated a partner's book?     | `partner_penetration`                      |

## Zoho-native path (recommended)

```text
HubSpot ──▶ Zoho HubSpot connector ──▶ stg_* Query Tables ──▶ int_* ──▶ partner_*
                                                                          │
                                                                          ▼
                                                                   Zoho dashboards
```

Full walkthrough: [`zoho/README.md`](zoho/README.md). ~30 min end to end:
connect HubSpot, upload two seed CSVs, create 18 Query Tables in order, build
dashboards using [`docs/dashboard_setup_zoho.md`](docs/dashboard_setup_zoho.md).

## dbt + warehouse path

```bash
make install                                  # dbt + Python deps
cp profiles.yml.example ~/.dbt/profiles.yml   # fill in warehouse creds
make build                                    # seed + run + test
```

For a live dashboard, schedule `make refresh` every 30–60 min — that runs
`dbt build` and then pushes the mart tables to Zoho Analytics. The mart tables
`partner_leads` / `partner_deals` are incremental with `merge`, so re-runs are
cheap and idempotent.

## Live dashboard in Zoho Analytics

Two paths — see [`docs/zoho_analytics_setup.md`](docs/zoho_analytics_setup.md)
for the full walkthrough.

- **Path A — Zoho native warehouse connector** (recommended for BigQuery,
  Snowflake, Redshift, Postgres). Point Zoho at the `partner_reporting_marts`
  schema; it syncs on its own schedule. Zero code.
- **Path B — API push**. Run `make bootstrap` once, then `make sync` (or let
  the GitHub Actions workflow at `.github/workflows/refresh.yml` do it every
  30 min). Uses `scripts/zoho_sync.py` to upsert via Zoho's Bulk Import API.

[`docs/dashboard_setup_zoho.md`](docs/dashboard_setup_zoho.md) walks through
building each dashboard page in Zoho once the tables are syncing.

## Adding or updating data

- **New partner**: add a row to `seeds/ref_partners.csv` → `dbt seed`.
- **New partner customer counts**: append rows to `seeds/partner_total_customers.csv`
  with a fresh `as_of_date` → `dbt seed` → `partner_penetration` picks it up.
- **Different HubSpot property names**: override `vars:` in `dbt_project.yml`
  or via `dbt build --vars '{partner_name_contact_property: my_custom_field}'`.

## Documentation

- [`docs/data_model.md`](docs/data_model.md) — ER diagram, column reference,
  join keys.
- [`docs/assumptions.md`](docs/assumptions.md) — every business rule we
  encoded (lifecycle stage mapping, attribution priority, edge cases).
- [`docs/dashboard_structure.md`](docs/dashboard_structure.md) — proposed
  dashboard pages, charts, and the tables each chart binds to.
- [`docs/zoho_analytics_setup.md`](docs/zoho_analytics_setup.md) — end-to-end
  Zoho Analytics connection guide (native connector or API push).
- [`docs/dashboard_setup_zoho.md`](docs/dashboard_setup_zoho.md) — Zoho-specific
  dashboard build recipes.
- [`docs/metrics_glossary.md`](docs/metrics_glossary.md) — exact metric formulas.
