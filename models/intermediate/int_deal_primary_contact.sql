{{ config(materialized='ephemeral') }}

-- Resolves exactly one "primary contact" per deal.
--   * If HubSpot has an is_primary flag set on the association, use it.
--   * Otherwise, fall back to the earliest-associated contact (deterministic).

with deal_contacts as (
    select * from {{ ref('stg_hubspot__deal_contacts') }}
),

contacts as (
    select contact_id, contact_created_at from {{ ref('stg_hubspot__contacts') }}
),

joined as (
    select
        dc.deal_id,
        dc.contact_id,
        dc.is_primary_contact,
        c.contact_created_at
    from deal_contacts dc
    left join contacts c using (contact_id)
),

ranked as (
    select
        *,
        row_number() over (
            partition by deal_id
            order by
                case when is_primary_contact then 0 else 1 end,
                contact_created_at asc nulls last,
                contact_id asc
        ) as rn
    from joined
)

select
    deal_id,
    contact_id as primary_contact_id
from ranked
where rn = 1
