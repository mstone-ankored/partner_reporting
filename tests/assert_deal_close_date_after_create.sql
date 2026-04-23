-- Deals should never have close_date < create_date. If this fires, investigate
-- the upstream HubSpot record: often a CRM admin manually backdated a deal.
-- Surfaced as a warning (not an error) so dbt build stays green; the offending
-- deal_ids still show up in the warning output for follow-up in HubSpot.
{{ config(severity = 'warn') }}

select
    deal_id,
    deal_created_at,
    deal_close_date
from {{ ref('partner_deals') }}
where deal_close_date is not null
  and deal_close_date < deal_created_at
