# Data Model

## Layered architecture

We follow the standard dbt convention of three layers:

1. **Staging** (`models/staging/`, materialized as `view`) — one model per raw
   HubSpot table. Renames columns, applies types, trims strings. Contains no
   business logic. Runs cheaply because it's a view.
2. **Intermediate** (`models/intermediate/`, materialized `ephemeral`) —
   business logic building blocks that are reused by multiple marts (partner
   attribution, first-touch detection, stage durations, sales touches,
   primary contact resolution). Ephemeral so they inline into consumers at
   compile time — zero storage cost.
3. **Marts** (`models/marts/`, materialized `table` or `incremental`) — the
   tables the dashboard reads.

## Entity–relationship overview

```
            ┌────────────────┐           ┌──────────────────┐
            │ ref_partners   │           │ partner_total_   │
            │ (seed)         │◀──────────│ customers (seed) │
            └───────┬────────┘           └──────────────────┘
                    │
                    │ partner_id / partner_name_key
                    ▼
┌──────────────────────────────────┐
│ int_partner_contact_attribution  │ (one row per partner-sourced contact)
└───────┬──────────────────────────┘
        │ contact_id
        ▼
┌──────────────┐         ┌──────────────────────┐       ┌──────────────────┐
│ partner_leads│◀────────│ stg_hubspot__contacts│       │ stg_hubspot__    │
│              │         └──────────┬───────────┘       │ form_submissions │
└──────┬───────┘                    │                   └─────────┬────────┘
       │                            │                             │
       │ contact_id                 │ contact_id                  │ contact_id
       ▼                            ▼                             │
┌──────────────┐  ┌──────────────────────┐  ┌─────────────────────▼────────┐
│ partner_deals│  │ int_contact_first_   │  │ (form_meta joined into       │
│              │  │ touch                │  │  partner_leads for form leads)│
└──────┬───────┘  └──────────────────────┘  └──────────────────────────────┘
       │ deal_id
       ▼
┌──────────────────────────────┐     ┌──────────────────────────────────┐
│ int_deal_stage_durations     │────▶│ partner_funnel_stage_conversion  │
│ int_deal_sales_touches       │     │ partner_deal_stage_durations     │
│ int_deal_primary_contact     │     └──────────────────────────────────┘
└──────────────────────────────┘
                    │
                    ▼
             ┌────────────────┐
             │ partner_summary│  ◀─ aggregates leads + deals per partner × period
             └───────┬────────┘
                     │
                     ▼
             ┌────────────────┐       ┌────────────────────┐
             │ partner_rankings│      │ partner_penetration │
             └─────────────────┘      └────────────────────┘
             ┌────────────────────────┐
             │ partner_rep_performance│  ◀─ owner × partner pivot of deals
             └────────────────────────┘
```

## Join keys

| Source                         | Target                        | Key                             |
|--------------------------------|-------------------------------|---------------------------------|
| `stg_hubspot__contacts`        | `stg_hubspot__deal_contacts`  | `contact_id`                    |
| `stg_hubspot__deal_contacts`   | `stg_hubspot__deals`          | `deal_id`                       |
| `stg_hubspot__engagements`     | `stg_hubspot__engagement_contacts` | `engagement_id`            |
| `stg_hubspot__engagement_contacts` | `stg_hubspot__contacts`   | `contact_id`                    |
| `stg_hubspot__form_submissions`| `stg_hubspot__contacts`       | `contact_id`                    |
| `stg_hubspot__deals`           | `stg_hubspot__owners`         | `deal_owner_id = owner_id`      |
| Any partner-bearing model      | `ref_partners`                | `lower(trim(partner_name)) = partner_name_key` |

## Handling multiple deals per contact

`partner_leads` is 1:1 with contact. `partner_deals` is 1:1 with deal — a
contact with three deals produces three rows. Downstream aggregations
(`partner_summary`, `partner_rankings`) count `distinct deal_id` when measuring
deal volume and `distinct contact_id` when measuring lead volume, so multi-deal
contacts do not distort either metric.

## Handling missing partner data

- **No custom partner property**: attribution falls through to form submissions,
  then to partner-domain email matching, then to HubSpot's original_source drill-down.
- **Partner name spelling drift**: all partner_name comparisons use
  `lower(trim(…)) = partner_name_key`, so "Acme Partners", "acme partners", and
  " Acme Partners " all match. The seed's spelling is canonical.
- **Partner seen in HubSpot but not in the seed**: `partner_leads.partner_id`
  will be null for those rows, and the `assert_partner_leads_match_partners_seed`
  test surfaces them so you can onboard them.
- **Deal has no associated contact**: excluded from `partner_deals` because we
  cannot attribute it (matches the HubSpot truth — uncontacted deals are rare).

## Incremental strategy

- `partner_leads` and `partner_deals` use `materialized: incremental,
  incremental_strategy: merge, unique_key: <id>`.
- They re-process rows where `_synced_at >= now() - var('incremental_lookback_days')`
  (default 14 days). Lookback covers late-arriving updates to HubSpot
  properties (e.g. a deal that flips to closed_won a week after creation).
- On schema change, new columns are appended (`on_schema_change: append_new_columns`).
- `dbt build --full-refresh` rebuilds from scratch; do this quarterly or when
  attribution logic changes.

## Performance

- Staging is all views, so it never reads twice in a single `dbt build`.
- Intermediate is ephemeral — no I/O.
- Marts are physical tables. `partner_summary` fans out one row per partner ×
  (month + quarter + all-time), which is small (≤ O(100k) even for big
  partners × multi-year histories).
- Warehouse partitioning hints: in BigQuery, partition `partner_leads` and
  `partner_deals` by `date(lead_created_at)` and `date(deal_created_at)`
  respectively. Add to model config if scale demands it.
