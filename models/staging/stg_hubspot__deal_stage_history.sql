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
        safe_cast(entered_at as timestamp)                          as entered_at,
        safe_cast(exited_at as timestamp)                           as exited_at
    from source
),

with_duration as (
    select
        deal_id,
        deal_stage,
        entered_at,
        exited_at,
        coalesce(exited_at, current_timestamp())                    as effective_exited_at,
        timestamp_diff(coalesce(exited_at, current_timestamp()),
                       entered_at, second)                          as seconds_in_stage
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
    safe_divide(seconds_in_stage, 86400)                             as days_in_stage
from with_duration
