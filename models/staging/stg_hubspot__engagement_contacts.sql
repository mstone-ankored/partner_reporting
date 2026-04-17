{{ config(materialized='view') }}

select
    engagement_id,
    contact_id
from {{ source('hubspot', 'engagement_contact') }}
where engagement_id is not null
  and contact_id is not null
