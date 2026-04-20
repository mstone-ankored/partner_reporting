{{ config(materialized='ephemeral') }}

-- Determines, for every contact, whether they are partner-sourced and if so:
--   * partner_name (canonicalized against ref_partners)
--   * source_type  ('partner_email' | 'form')
--   * partner_attribution_method (how we identified them; for debugging/audit)
--
-- Attribution priority (first match wins, highest confidence to lowest):
--   1. HubSpot custom contact property `partner_source_type_declared` explicitly
--      set to 'partner_email' or 'form' AND a partner_name is also present.
--   2. Form submission with a partner_name form field OR a partner-referral form.
--   3. HubSpot custom `referring_partner_name_declared` contact property populated.
--   4. Inbound engagement email from a known partner domain (ref_partners.partner_domain).
--   5. HubSpot original_source = 'REFERRALS' with drill-down matching a partner name.
--
-- A contact with no attribution match is NOT returned. Downstream models can
-- left-join if they need all contacts.

with contacts as (
    select * from {{ ref('stg_hubspot__contacts') }}
),

partners as (
    select * from {{ ref('stg_ref__partners') }}
),

forms as (
    select * from {{ ref('stg_hubspot__form_submissions') }}
),

engagements as (
    select * from {{ ref('stg_hubspot__engagements') }}
),

engagement_contacts as (
    select * from {{ ref('stg_hubspot__engagement_contacts') }}
),

-- -----------------------------------------------------------------------------
-- METHOD 1 + 3: explicit custom contact property on HubSpot.
-- -----------------------------------------------------------------------------
declared_property as (
    select
        c.contact_id,
        p.partner_id,
        p.partner_name,
        case
            when c.partner_source_type_declared in ('partner_email', 'email', 'referral_email') then 'partner_email'
            when c.partner_source_type_declared in ('form', 'form_submission')                   then 'form'
            else null
        end as declared_source_type,
        c.contact_created_at
    from contacts c
    left join partners p
      on lower(trim(c.referring_partner_name_declared)) = p.partner_name_key
    where c.referring_partner_name_declared is not null
),

-- -----------------------------------------------------------------------------
-- METHOD 2: partner attribution via form submission.
--   * Either the form submission itself carried a partner_name field, or the
--     form is one of the configured partner-referral forms. We take the
--     earliest such submission per contact.
-- -----------------------------------------------------------------------------
form_attribution as (
    select
        f.contact_id,
        p.partner_id,
        coalesce(p.partner_name, f.partner_name_from_form)         as partner_name,
        f.submission_id,
        f.form_id,
        f.form_name,
        f.submitted_at,
        row_number() over (
            partition by f.contact_id
            order by f.submitted_at asc, f.submission_id asc
        ) as rn
    from forms f
    left join partners p
      on lower(trim(f.partner_name_from_form)) = p.partner_name_key
    where f.is_partner_referral_form = true
),

form_attribution_first as (
    select * from form_attribution where rn = 1
),

-- -----------------------------------------------------------------------------
-- METHOD 4: inbound email engagement from a known partner domain.
--   * Engagement type 'email', direction 'incoming', from_domain matches a
--     domain in ref_partners. We take the earliest such engagement per contact.
-- -----------------------------------------------------------------------------
partner_email_engagements as (
    select
        ec.contact_id,
        p.partner_id,
        p.partner_name,
        e.engagement_id,
        e.engaged_at,
        e.email_from_domain,
        row_number() over (
            partition by ec.contact_id
            order by e.engaged_at asc, e.engagement_id asc
        ) as rn
    from engagements e
    join engagement_contacts ec using (engagement_id)
    join partners p
      on e.email_from_domain = p.partner_domain
    where e.engagement_type = 'email'
      and e.email_direction = 'incoming'
),

partner_email_first as (
    select * from partner_email_engagements where rn = 1
),

-- -----------------------------------------------------------------------------
-- METHOD 5: HubSpot original_source = 'REFERRALS' with matching drill-down.
-- -----------------------------------------------------------------------------
referral_source as (
    select
        c.contact_id,
        p.partner_id,
        p.partner_name
    from contacts c
    join partners p
      on lower(trim(c.original_source_drill_down_1)) = p.partner_name_key
      or lower(trim(c.original_source_drill_down_2)) = p.partner_name_key
    where upper(c.original_source) in ('REFERRALS', 'REFERRAL')
),

-- -----------------------------------------------------------------------------
-- Combine: one row per contact, taking highest-priority match.
-- -----------------------------------------------------------------------------
unioned as (
    select
        contact_id,
        partner_id,
        partner_name,
        coalesce(declared_source_type, 'partner_email')  as source_type,
        'declared_property'                              as attribution_method,
        1                                                as priority,
        cast(null as timestamp)                          as evidence_at,
        cast(null as text)                             as evidence_id
    from declared_property

    union all

    select
        contact_id,
        partner_id,
        partner_name,
        'form'                                           as source_type,
        'form_submission'                                as attribution_method,
        2                                                as priority,
        submitted_at                                     as evidence_at,
        submission_id                                    as evidence_id
    from form_attribution_first

    union all

    select
        contact_id,
        partner_id,
        partner_name,
        'partner_email'                                  as source_type,
        'partner_email_domain'                           as attribution_method,
        3                                                as priority,
        engaged_at                                       as evidence_at,
        engagement_id                                    as evidence_id
    from partner_email_first

    union all

    select
        contact_id,
        partner_id,
        partner_name,
        'partner_email'                                  as source_type,
        'referral_original_source'                       as attribution_method,
        4                                                as priority,
        cast(null as timestamp)                          as evidence_at,
        cast(null as text)                             as evidence_id
    from referral_source
),

ranked as (
    select
        *,
        row_number() over (
            partition by contact_id
            order by priority asc, evidence_at asc nulls last
        ) as rn
    from unioned
    where partner_name is not null
)

select
    contact_id,
    partner_id,
    partner_name,
    source_type,
    attribution_method,
    evidence_at            as attribution_evidence_at,
    evidence_id            as attribution_evidence_id
from ranked
where rn = 1
