{{ config(materialized='ephemeral') }}

-- Counts of sales touches per deal, used for "sales touches required to close".
-- Touches are counted against the deal's primary contact (via engagement_contacts)
-- between deal_created_at and deal_close_date (or now() if still open).

with deals as (
    select d.deal_id, d.deal_created_at, d.deal_close_date, pc.primary_contact_id
    from {{ ref('stg_hubspot__deals') }} d
    left join {{ ref('int_deal_primary_contact') }} pc using (deal_id)
),

engagements as (
    select * from {{ ref('stg_hubspot__engagements') }}
),

engagement_contacts as (
    select * from {{ ref('stg_hubspot__engagement_contacts') }}
),

contact_engagements as (
    select
        ec.contact_id,
        e.engagement_id,
        e.engagement_type,
        e.engaged_at,
        e.email_direction
    from engagement_contacts ec
    join engagements e using (engagement_id)
),

joined as (
    select
        d.deal_id,
        ce.engagement_id,
        ce.engagement_type,
        ce.engaged_at,
        ce.email_direction
    from deals d
    left join contact_engagements ce
      on ce.contact_id = d.primary_contact_id
     and ce.engaged_at >= d.deal_created_at
     and ce.engaged_at <= coalesce(d.deal_close_date, current_timestamp())
),

agg as (
    select
        deal_id,
        count(distinct engagement_id)                                           as total_touches,
        count(distinct case when engagement_type = 'email' and email_direction = 'outgoing' then engagement_id end) as outbound_emails,
        count(distinct case when engagement_type = 'call'                                    then engagement_id end) as calls,
        count(distinct case when engagement_type = 'meeting'                                 then engagement_id end) as meetings
    from joined
    group by 1
)

select * from agg
