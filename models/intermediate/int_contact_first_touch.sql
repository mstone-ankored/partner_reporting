{{ config(materialized='ephemeral') }}

-- First-activity and first-sales-touch timestamps per contact.
--   * first_touch_at: earliest engagement of any type
--   * first_sales_touch_at: earliest OUTGOING engagement from a sales rep
--     (email/call/meeting), used to compute "time to first sales touch".

with engagements as (
    select * from {{ ref('stg_hubspot__engagements') }}
),

engagement_contacts as (
    select * from {{ ref('stg_hubspot__engagement_contacts') }}
),

joined as (
    select
        ec.contact_id,
        e.engagement_id,
        e.engagement_type,
        e.engaged_at,
        e.email_direction,
        e.engagement_owner_id
    from engagement_contacts ec
    join engagements e using (engagement_id)
),

first_touch as (
    select
        contact_id,
        min(engaged_at) as first_touch_at
    from joined
    group by 1
),

first_sales_touch as (
    select
        contact_id,
        min(engaged_at) as first_sales_touch_at
    from joined
    where engagement_type in ('call', 'meeting')
       or (engagement_type = 'email' and email_direction = 'outgoing')
    group by 1
)

select
    coalesce(ft.contact_id, fs.contact_id) as contact_id,
    ft.first_touch_at,
    fs.first_sales_touch_at
from first_touch ft
full outer join first_sales_touch fs using (contact_id)
