{{ config(materialized='view') }}

-- One row per (contact, form submission). Surface the form fields we need for
-- partner attribution and lead-quality reporting. Fivetran lands form values
-- as a JSON string in `submission_values`; if your loader splits them into
-- columns, adjust the JSON_VALUE calls below to direct column refs.

with source as (
    select * from {{ source('hubspot', 'contact_form') }}
),

forms as (
    select * from {{ source('hubspot', 'form') }}
),

joined as (
    select
        s.conversion_id                                             as submission_id,
        s.contact_id,
        s.form_id,
        f.name                                                      as form_name,
        safe_cast(s.submitted_at as timestamp)                      as submitted_at,
        s.page_url                                                  as submission_page_url,
        s.submission_values                                         as submission_values_raw
    from source s
    left join forms f using (form_id)
),

extracted as (
    select
        submission_id,
        contact_id,
        form_id,
        form_name,
        submitted_at,
        submission_page_url,
        submission_values_raw,
        -- Partner attribution fields on the form. We check a configurable list
        -- of field names and coalesce the first non-null value.
        coalesce(
            {% for f in var('partner_form_fields') -%}
                nullif(trim(json_value(submission_values_raw, '$.{{ f }}')), ''){% if not loop.last %},{% endif %}
            {% endfor %}
        ) as partner_name_from_form,
        -- Generic firmographic fields frequently collected on forms.
        nullif(json_value(submission_values_raw, '$.company'), '')        as form_company_name,
        nullif(json_value(submission_values_raw, '$.company_size'), '')   as form_company_size,
        nullif(json_value(submission_values_raw, '$.numberofemployees'), '') as form_num_employees,
        nullif(json_value(submission_values_raw, '$.industry'), '')       as form_industry,
        nullif(json_value(submission_values_raw, '$.country'), '')        as form_country,
        nullif(json_value(submission_values_raw, '$.use_case'), '')       as form_use_case
    from joined
),

flagged as (
    select
        *,
        case
            when partner_name_from_form is not null then true
            when lower(form_name) in (
                {{ "'" ~ var('partner_referral_form_names') | map('lower') | join("','") ~ "'" }}
            ) then true
            else false
        end as is_partner_referral_form
    from extracted
)

select * from flagged
