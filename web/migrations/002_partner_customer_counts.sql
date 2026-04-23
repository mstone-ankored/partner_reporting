-- Partner customer counts. Hand-entered in the web app at /settings/partners
-- and read by dbt (via stg_ref__partner_total_customers) to compute partner
-- penetration. Replaces the placeholder seeds/partner_total_customers.csv as
-- the source of truth.

create table if not exists partner_reporting_app.partner_customer_counts (
    id                    uuid primary key default gen_random_uuid(),
    partner_id            text not null,
    as_of_date            date not null,
    total_customer_count  integer not null check (total_customer_count >= 0),
    notes                 text,
    created_by            uuid references partner_reporting_app.users(id),
    created_at            timestamp not null default now(),
    updated_at            timestamp not null default now(),
    unique (partner_id, as_of_date)
);
create index if not exists partner_customer_counts_partner_idx
    on partner_reporting_app.partner_customer_counts(partner_id, as_of_date desc);
