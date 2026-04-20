{{ config(materialized='view') }}

select
    deal_id,
    contact_id,
    coalesce({{ safe_cast('is_primary', 'bool') }}, false) as is_primary_contact
from {{ source('hubspot', 'deal_contact') }}
where deal_id is not null and contact_id is not null
