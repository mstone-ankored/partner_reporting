{{ config(materialized='view') }}

-- One row per HubSpot engagement (email/call/meeting/note/task).
-- Used for: first-touch timestamp, count of sales touches per deal, and
-- identifying inbound partner emails.

with source as (
    select * from {{ source('hubspot', 'engagement') }}
),

renamed as (
    select
        engagement_id,
        lower(engagement_type)                                      as engagement_type,
        {{ safe_cast('engagement_timestamp', 'timestamp') }}        as engaged_at,
        owner_id                                                    as engagement_owner_id,
        engagement_source                                           as engagement_source,
        lower(email_metadata_direction)                             as email_direction,  -- 'incoming'|'outgoing'
        lower(email_metadata_from_email)                            as email_from_address,
        {{ regex_extract_group('lower(email_metadata_from_email)', '@([^@]+)$') }} as email_from_domain,
        email_metadata_subject                                      as email_subject,
        _fivetran_synced                                            as _synced_at
    from source
)

select * from renamed
