{{ config(materialized='view') }}

-- One row per (deal, stage transition). Produced by flattening the dealstage
-- property history. If your pipeline lands this as a separate table, great;
-- if you only have property_history, unnest it here.

with source as (
    select * from {{ source('hubspot', 'deal_stage_history') }}
),

renamed as (
    select
        deal_id                                                     as deal_id,
        lower(stage_id)                                             as deal_stage,
        entered_at::timestamp                                       as entered_at,
        exited_at::timestamp                                        as exited_at
    from source
),

with_duration as (
    select
        deal_id,
        deal_stage,
        entered_at,
        exited_at,
        coalesce(exited_at, now())                                  as effective_exited_at,
        {{ timestamp_diff_seconds('coalesce(exited_at, now())', 'entered_at') }} as seconds_in_stage
    from renamed
    where entered_at is not null
)

select
    deal_id,
    deal_stage,
    entered_at,
    exited_at,
    effective_exited_at,
    seconds_in_stage,
    {{ safe_divide('seconds_in_stage', '86400') }}                   as days_in_stage
from with_duration
