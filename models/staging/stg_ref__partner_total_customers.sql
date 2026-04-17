{{ config(materialized='view') }}

-- Time-series of total customer counts per partner. Used to compute partner
-- penetration (our customers / partner's total customers) over time.

select
    partner_id,
    partner_name,
    lower(trim(partner_name)) as partner_name_key,
    as_of_date,
    total_customer_count
from {{ ref('partner_total_customers') }}
