{{
    config(
        materialized='incremental',
        unique_key='contact_id',
        on_schema_change='append_new_columns',
        incremental_strategy='merge'
    )
}}

-- partner_leads: one row per partner-sourced inbound lead (contact).
-- Produced by joining contact attribution to contact attributes, first-touch
-- timestamps, and (optionally) the most recent partner-referral form submission
-- for form-sourced leads.
--
-- Incremental strategy:
--   * Full refresh on first build.
--   * On subsequent runs, only re-process contacts whose underlying source row
--     was updated in the last `incremental_lookback_days` days. The MERGE
--     statement upserts by contact_id so reruns are idempotent.

with attribution as (
    select * from {{ ref('int_partner_contact_attribution') }}
),

contacts as (
    select * from {{ ref('stg_hubspot__contacts') }}
    {% if is_incremental() %}
      where _synced_at >= now() - interval '{{ var('incremental_lookback_days') }} days'
    {% endif %}
),

forms as (
    select * from {{ ref('stg_hubspot__form_submissions') }}
),

first_touch as (
    select * from {{ ref('int_contact_first_touch') }}
),

-- For form-sourced leads, grab the earliest partner-referral form submission
-- to expose form metadata (company size, industry, etc).
form_meta as (
    select
        contact_id,
        form_id,
        form_name,
        submitted_at,
        form_company_name,
        form_company_size,
        form_num_employees,
        form_industry,
        form_country,
        form_use_case,
        row_number() over (
            partition by contact_id
            order by submitted_at asc, submission_id asc
        ) as rn
    from forms
    where is_partner_referral_form = true
),

form_meta_first as (
    select * from form_meta where rn = 1
),

lifecycle_flags as (
    select
        c.contact_id,
        case
            when lower(c.lifecycle_stage) in ({{ "'" ~ var('mql_lifecycle_values') | join("','") ~ "'" }})
              or c.became_mql_at is not null then true
            else false
        end as reached_mql,
        case
            when lower(c.lifecycle_stage) in ({{ "'" ~ var('sql_lifecycle_values') | join("','") ~ "'" }})
              or c.became_sql_at is not null then true
            else false
        end as reached_sql,
        case
            when lower(c.lifecycle_stage) in ({{ "'" ~ var('disqualified_lifecycle_values') | join("','") ~ "'" }})
              or lower(c.lead_status) in ('unqualified', 'disqualified', 'bad_fit', 'dq')
            then true
            else false
        end as is_disqualified
    from contacts c
)

select
    -- Identity
    c.contact_id,
    a.partner_id,
    a.partner_name,
    a.source_type,
    a.attribution_method,
    a.attribution_evidence_at,

    -- Lead / contact attributes
    c.email,
    c.email_domain,
    c.first_name,
    c.last_name,
    c.company_name,
    c.job_title,
    c.industry,
    c.company_size_employees,
    c.company_annual_revenue,
    c.contact_owner_id,

    -- Dates
    c.contact_created_at                              as lead_created_at,
    c.contact_created_at::date                        as lead_created_date,
    date_trunc('month',   c.contact_created_at)::date as lead_created_month,
    date_trunc('quarter', c.contact_created_at)::date as lead_created_quarter,
    ft.first_touch_at,
    ft.first_sales_touch_at,
    {{ timestamp_diff_seconds('ft.first_sales_touch_at', 'c.contact_created_at') }} / 3600.0 as hours_to_first_sales_touch,

    -- Lifecycle
    c.lifecycle_stage,
    c.lead_status,
    c.became_lead_at,
    c.became_mql_at,
    c.became_sql_at,
    c.became_opportunity_at,
    c.became_customer_at,
    -- MQL here means: HubSpot lifecycle crossed into MQL *or* the lead came in
    -- through a partner-referral form (fm.form_id set below). Partner forms
    -- are treated as MQL-by-definition for partner reporting even when the
    -- HubSpot lifecycle_stage hasn't been bumped yet.
    coalesce(lf.reached_mql, false) or fm.form_id is not null  as reached_mql,
    lf.reached_sql,
    lf.is_disqualified,

    -- Form metadata (null for partner_email leads)
    fm.form_id,
    fm.form_name,
    fm.submitted_at                                   as form_submitted_at,
    fm.form_company_name,
    fm.form_company_size,
    fm.form_num_employees,
    fm.form_industry,
    fm.form_country,
    fm.form_use_case,

    -- Source attribution (HubSpot native)
    c.original_source,
    c.original_source_drill_down_1,
    c.original_source_drill_down_2,

    c._synced_at
from contacts c
join attribution a using (contact_id)
left join first_touch ft using (contact_id)
left join form_meta_first fm using (contact_id)
left join lifecycle_flags lf using (contact_id)
