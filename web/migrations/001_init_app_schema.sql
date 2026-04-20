-- App-owned schema. Holds auth + notion sync config. Kept separate from the
-- dbt-owned `partner_reporting` schema so that `dbt build --full-refresh`
-- doesn't touch user accounts.

create schema if not exists partner_reporting_app;

create table if not exists partner_reporting_app.users (
    id              uuid primary key default gen_random_uuid(),
    email           text unique not null,
    password_hash   text not null,
    name            text,
    role            text not null default 'member',
    created_at      timestamp not null default now(),
    last_login_at   timestamp
);

create table if not exists partner_reporting_app.sessions (
    token       text primary key,
    user_id     uuid not null references partner_reporting_app.users(id) on delete cascade,
    expires_at  timestamp not null,
    created_at  timestamp not null default now()
);
create index if not exists sessions_user_idx on partner_reporting_app.sessions(user_id);

-- Notion sync config — one row per mart table the user wants to push. The UI
-- at /settings/notion writes these rows; scripts/notion_sync.py reads them.
create table if not exists partner_reporting_app.notion_sync_targets (
    id                  uuid primary key default gen_random_uuid(),
    source_table        text not null,     -- e.g. 'partner_rankings'
    notion_database_id  text not null,     -- destination Notion DB id
    filter_json         jsonb not null default '{}'::jsonb,   -- {"period_type":"all_time"} etc
    column_map_json     jsonb not null default '{}'::jsonb,   -- {"partner_name":"Partner", ...}
    enabled             boolean not null default true,
    last_synced_at      timestamp,
    last_sync_status    text,
    last_sync_message   text,
    created_by          uuid references partner_reporting_app.users(id),
    created_at          timestamp not null default now(),
    updated_at          timestamp not null default now()
);

-- User-saved forecast scenarios (pipeline projections). Stored as JSON so the
-- schema can evolve without migrations as we add forecast dimensions.
create table if not exists partner_reporting_app.forecast_scenarios (
    id            uuid primary key default gen_random_uuid(),
    name          text not null,
    partner_id    text,               -- null = all partners
    assumptions   jsonb not null,     -- {growth_pct, seasonality, horizon_months, ...}
    created_by    uuid references partner_reporting_app.users(id),
    created_at    timestamp not null default now(),
    updated_at    timestamp not null default now()
);
