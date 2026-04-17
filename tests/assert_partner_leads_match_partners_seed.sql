-- Warn if partner_leads contains partner_names that do not exist in ref_partners.
-- A non-empty result is a signal to onboard the partner into the seed.

select
    pl.partner_name,
    count(*) as leads_with_unmapped_partner
from {{ ref('partner_leads') }} pl
left join {{ ref('stg_ref__partners') }} p
  on lower(trim(pl.partner_name)) = p.partner_name_key
where p.partner_id is null
group by 1
