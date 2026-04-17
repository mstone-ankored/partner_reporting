# Assumptions and Business Rules

Every non-obvious decision encoded in the models is listed here. If your org's
definition differs, override via dbt vars or modify the model — but update
this doc so reviewers can audit the change.

## 1. Partner identification

A lead is considered partner-sourced if **any** of the following hold (in
priority order, first match wins):

| Priority | Method                     | Evidence                                                                |
|----------|----------------------------|--------------------------------------------------------------------------|
| 1        | `declared_property`        | HubSpot contact property `referring_partner_name` is populated.          |
| 2        | `form_submission`          | Form submission carries a `partner_name` / `referring_partner` field, OR the form is listed in `var('partner_referral_form_names')`. |
| 3        | `partner_email_domain`     | Inbound email engagement from a domain in `ref_partners.partner_domain`. |
| 4        | `referral_original_source` | HubSpot `original_source = 'REFERRALS'` AND drill-down matches a partner. |

The priority order is tuned to give **declared CRM data** precedence over
inferred signals. If the partner team manually sets `referring_partner_name`,
that beats anything we could infer from email traffic.

**Source type classification**:
- `partner_email` — priorities 1 (when declared property says so), 3, 4.
- `form` — priority 2, and priority 1 if the declared property explicitly says 'form'.

## 2. Lifecycle stage → lead quality

Lead quality metrics rely on HubSpot's `lifecyclestage` property. Mappings live
in `dbt_project.yml → vars`. Defaults:

| Flag                 | Values                                             |
|----------------------|----------------------------------------------------|
| `reached_mql`        | `marketingqualifiedlead`, `mql`                    |
| `reached_sql`        | `salesqualifiedlead`, `sql`                        |
| `is_disqualified`    | `other`, `disqualified`, OR `lead_status` in [`unqualified`, `disqualified`, `bad_fit`, `dq`] |

We look at **both** the current stage AND the historical `hs_lifecyclestage_*_date`
timestamps — so a lead that reached MQL and later progressed to SQL still
contributes to both MQL and SQL rates.

## 3. Funnel stages and drop-off

Stage order is configured via `var('deal_stage_order')`:

```
discovery → demo → proposal → negotiation → closed_won
```

For each partner × stage, we compute:
- `deals_entered` — distinct deals that entered the stage (including re-entries).
- `deals_advanced` — distinct deals whose max stage reached was strictly later.
- `next_stage_conversion_rate` — `deals_entered_next_stage / deals_entered_this_stage`.
- `drop_off_rate` — `1 - next_stage_conversion_rate`. The stage with the
  highest `drop_off_rate` per partner is their biggest leak.

If your pipeline has a different stage order, update
`var('deal_stage_order')` in `dbt_project.yml`; the funnel + rankings models
re-rank automatically.

## 4. Deal → contact association

A deal can be associated to many contacts in HubSpot. We pick exactly one
"primary contact" per deal:

1. If any association is marked `is_primary = true`, use that one.
2. Otherwise, use the contact with the earliest `contact_created_at`.
3. Ties broken deterministically by `contact_id asc`.

The primary contact is used for (a) inheriting partner attribution onto the
deal and (b) scoping sales-touch counts to engagements with that contact.

## 5. Multiple deals per contact

Preserved in `partner_deals`. A single lead with three deals produces three
rows. Aggregates use `count(distinct deal_id)` and `count(distinct contact_id)`
to avoid double-counting.

## 6. Revenue and close

- `revenue_closed_won` = `SUM(amount)` over deals where `is_closed_won = true`.
- `is_closed_won` comes directly from HubSpot's `hs_is_closed_won` when present;
  otherwise inferred from `deal_stage ∈ var('closed_won_stages')`.
- `time_to_close_days` = `deal_closed_won_at – deal_created_at`, falling back
  to `deal_close_date – deal_created_at` if the won-timestamp is missing. For
  still-open deals, uses `current_timestamp()` so dashboards can still plot
  "deals getting stale".

## 7. Sales touches

A sales touch against a deal is any engagement associated with the deal's
primary contact **between** `deal_created_at` and `deal_close_date` (or now(),
for open deals). We count:
- Outgoing emails
- Calls
- Meetings

Notes and tasks are excluded because they are not customer-visible.

## 8. Revenue per lead / per deal

- `revenue_per_lead` = `revenue_closed_won / total_leads` for the period.
- `revenue_per_closed_won_deal` = `revenue_closed_won / deals_closed_won`.
- Both are computed in the period where the **close** happened, not where the
  lead was created. For a cohort-based view (revenue attributable to leads
  created in a given month), use `partner_lead_cohorts`.

## 9. Volume vs efficiency quadrant

`partner_rankings.volume_efficiency_quadrant` is computed over the all-time
summary row. Cutpoints are the **median** across partners of:
- `total_leads` (volume axis)
- `lead_to_won_rate` (efficiency axis)

Median is preferred over mean to avoid one extreme partner shifting the
classification of everyone else.

- `high_volume_high_conversion` → top performers; double-down.
- `high_volume_low_conversion` → leaky funnel; coach or re-qualify leads.
- `low_volume_high_conversion` → **high-potential**; flag for investment.
- `low_volume_low_conversion` → deprioritize or offboard.

## 10. Partner penetration

`penetration_rate = our_customer_count / total_customer_count`.

- `total_customer_count` comes from the `partner_total_customers` seed (the
  partner team owns keeping it current; can also be populated via Airbyte /
  an external CSV dropped into the seed folder by a workflow).
- `our_customer_count` = distinct closed-won deals per partner in
  `partner_deals`. We use a company-level dedup (`deal_name` lowercased) to
  avoid counting expansion deals as multiple customers.
- One row per `(partner_id, as_of_date)`, so the dashboard can trend
  penetration over time without schema changes.

## 11. Edge cases handled explicitly

- **Backdated deals** (`deal_close_date < deal_created_at`): flagged by the
  `assert_deal_close_date_after_create` data test.
- **Leads with no engagement at all**: `first_touch_at` is null; the
  `avg_hours_to_first_sales_touch` calculation simply excludes them.
- **Partner in HubSpot but missing from the seed**: leads still land in
  `partner_leads` with `partner_id = null`; surfaced by the
  `assert_partner_leads_match_partners_seed` test.
- **Deal with zero contacts**: excluded from `partner_deals` (cannot attribute).
- **Form submission tied to a contact that no longer exists**: handled by the
  left-joins; the submission is dropped silently.

## 12. What this system intentionally does NOT do

- **Multi-touch attribution** — we assign each contact a single partner via
  highest-priority evidence. If you need fractional multi-partner credit,
  extend `int_partner_contact_attribution` to emit (contact, partner, weight)
  triples.
- **Deal-level partner override per deal** is supported when the deal carries
  its own custom `referring_partner_name` property, but contact-level
  attribution wins when both are present (higher fidelity since it carries
  source_type).
- **Identity resolution across contacts** — we assume one HubSpot contact =
  one human. If you dedupe on email domain or name, do it upstream.
