{{ config(materialized='view') }}

select
    owner_id,
    email                                         as owner_email,
    first_name                                    as owner_first_name,
    last_name                                     as owner_last_name,
    trim(concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))) as owner_name,
    team_id                                       as owner_team_id,
    {{ safe_cast('created_at', 'timestamp') }}            as owner_created_at,
    coalesce({{ safe_cast('archived', 'bool') }}, false)  as is_archived
from {{ source('hubspot', 'owner') }}
