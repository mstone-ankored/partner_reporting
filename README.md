# Partner Performance Reporting

Production-ready analytics stack that turns raw HubSpot data (contacts, deals,
engagements, form submissions) into partner-level performance tables that
power a live dashboard.

This is a **dbt project**. It is designed to run on a schedule (e.g. hourly
via dbt Cloud, Airflow, GitHub Actions, or your orchestrator of choice) and
produces stable, incremental tables in your warehouse.

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

## Running locally

```bash
pip install dbt-bigquery      # or dbt-snowflake / dbt-postgres
cp profiles.yml.example ~/.dbt/profiles.yml   # fill in credentials

dbt deps
dbt seed         # loads ref_partners + partner_total_customers
dbt build        # runs models + tests in dependency order
```

For a live dashboard, schedule `dbt build --select state:modified+ source:hubspot+`
every 30–60 min. The mart tables `partner_leads` and `partner_deals` are
materialized `incremental` with `incremental_strategy: merge`, so re-runs are
cheap and idempotent.

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
