{{ config(materialized='view') }}

-- Time-series of total customer counts per partner. Used to compute partner
-- penetration (our customers / partner's total customers) over time.
--
-- Two sources, unioned:
--   1. partner_reporting_app.partner_customer_counts — hand-entered in the
--      web app at /settings/partners. Primary source of truth going forward.
--   2. seeds/partner_total_customers.csv — legacy / bootstrap rows; kept so
--      pre-app data (if any) still flows through until superseded by an app
--      entry for the same (partner_id, as_of_date).

with app_entries as (
    select
        pcc.partner_id,
        p.partner_name,
        p.partner_name_key,
        pcc.as_of_date,
        pcc.total_customer_count
    from {{ source('partner_reporting_app', 'partner_customer_counts') }} pcc
    join {{ ref('stg_ref__partners') }} p using (partner_id)
),

seed_entries as (
    select
        partner_id,
        partner_name,
        lower(trim(partner_name)) as partner_name_key,
        as_of_date,
        total_customer_count
    from {{ ref('partner_total_customers') }}
),

-- App entries take precedence when the same (partner_id, as_of_date) exists
-- in both sources.
merged as (
    select * from app_entries
    union all
    select s.*
    from seed_entries s
    where not exists (
        select 1 from app_entries a
        where a.partner_id = s.partner_id
          and a.as_of_date = s.as_of_date
    )
)

select * from merged
