"""
HubSpot → Neon ingestion.

Lands raw HubSpot objects into the `hubspot` schema using Fivetran-compatible
column names, so the downstream dbt staging models (`stg_hubspot__*`) consume
this data unchanged.

First run does a full scan; subsequent runs are incremental via
`hs_lastmodifieddate` tracked in `hubspot._sync_state`. Archived records are
also picked up and flagged with `_archived = true` so downstream models can
filter them out.

Synced objects → target tables:
  contacts            → hubspot.contact
  deals               → hubspot.deal (+ derived hubspot.deal_stage_history,
                                       hubspot.deal_contact)
  owners              → hubspot.owner
  emails / calls /
  meetings / notes /
  tasks               → hubspot.engagement (+ derived hubspot.engagement_contact)
  forms               → hubspot.form
  form submissions    → hubspot.contact_form

Requires env:
  DATABASE_URL           Neon pooled URL
  HUBSPOT_ACCESS_TOKEN   HubSpot private-app access token

Runs from the refresh workflow before `dbt build`.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

import psycopg2
import psycopg2.extras
import requests

API = "https://api.hubapi.com"
PAGE_SIZE = 100

log = logging.getLogger("hubspot_sync")


# -----------------------------------------------------------------------------
# Property lists (HubSpot internal names). Order does not matter.
# -----------------------------------------------------------------------------

CONTACT_PROPS = [
    "email", "firstname", "lastname", "company", "jobtitle", "industry",
    "numberofemployees", "annualrevenue",
    "lifecyclestage", "hs_lead_status",
    "createdate",
    "hs_lifecyclestage_lead_date",
    "hs_lifecyclestage_marketingqualifiedlead_date",
    "hs_lifecyclestage_salesqualifiedlead_date",
    "hs_lifecyclestage_opportunity_date",
    "hs_lifecyclestage_customer_date",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_source_data_2",
    "hs_latest_source",
    "hs_latest_source_data_1",
    "hs_latest_source_data_2",
    "hubspot_owner_id",
    # Partner attribution on contacts. `referring_partner` is the dropdown of
    # partner names; `lead_origin` / `lead_source` carry a "Partner Referral"
    # flag (used as a secondary signal when the dropdown isn't filled in).
    "referring_partner",
    "lead_origin",
    "lead_source",
    "partner_source",
    "hs_lastmodifieddate",
]

DEAL_PROPS = [
    "dealname", "pipeline", "dealstage", "hs_deal_stage_probability",
    "amount", "amount_in_home_currency",
    "createdate", "closedate", "hs_closed_won_date",
    "hs_is_closed_won", "hs_is_closed",
    "dealtype", "hubspot_owner_id",
    # Partner attribution on deals. `deal_source` = 'Partner Referral' is the
    # flag, `referring_partner` is the dropdown of which partner.
    "deal_source",
    "referring_partner",
    "deal_source_detail",
    "hs_lastmodifieddate",
]

# Shared engagement-object properties (emails / calls / meetings / notes / tasks).
ENGAGEMENT_COMMON_PROPS = [
    "hs_timestamp",
    "hubspot_owner_id",
    "hs_createdate",
    "hs_lastmodifieddate",
]
EMAIL_PROPS = ENGAGEMENT_COMMON_PROPS + [
    "hs_email_direction",
    "hs_email_from_email",
    "hs_email_subject",
]
CALL_PROPS = ENGAGEMENT_COMMON_PROPS + ["hs_call_title", "hs_call_direction"]
MEETING_PROPS = ENGAGEMENT_COMMON_PROPS + ["hs_meeting_title"]
NOTE_PROPS = ENGAGEMENT_COMMON_PROPS + ["hs_note_body"]
TASK_PROPS = ENGAGEMENT_COMMON_PROPS + ["hs_task_subject"]

ENGAGEMENT_OBJECTS = [
    # (hubspot object name, our engagement_type label, property list)
    ("emails", "email", EMAIL_PROPS),
    ("calls", "call", CALL_PROPS),
    ("meetings", "meeting", MEETING_PROPS),
    ("notes", "note", NOTE_PROPS),
    ("tasks", "task", TASK_PROPS),
]


# -----------------------------------------------------------------------------
# DDL
# -----------------------------------------------------------------------------

# Columns we know about per table. Extra custom properties on contacts / deals
# are stored as `property_<name>` text columns — added dynamically in _ensure_columns.

DDL_STATEMENTS = [
    "create schema if not exists hubspot",

    """create table if not exists hubspot._sync_state (
        object_name text primary key,
        last_sync_at timestamptz not null
    )""",

    """create table if not exists hubspot.contact (
        id text primary key,
        property_email text,
        property_firstname text,
        property_lastname text,
        property_company text,
        property_jobtitle text,
        property_industry text,
        property_numberofemployees text,
        property_annualrevenue text,
        property_lifecyclestage text,
        property_hs_lead_status text,
        property_createdate text,
        property_hs_lifecyclestage_lead_date text,
        property_hs_lifecyclestage_marketingqualifiedlead_date text,
        property_hs_lifecyclestage_salesqualifiedlead_date text,
        property_hs_lifecyclestage_opportunity_date text,
        property_hs_lifecyclestage_customer_date text,
        property_hs_analytics_source text,
        property_hs_analytics_source_data_1 text,
        property_hs_analytics_source_data_2 text,
        property_hs_latest_source text,
        property_hs_latest_source_data_1 text,
        property_hs_latest_source_data_2 text,
        property_hubspot_owner_id text,
        property_referring_partner text,
        property_lead_origin text,
        property_lead_source text,
        property_partner_source text,
        _fivetran_synced timestamptz not null default now(),
        _archived boolean not null default false
    )""",

    # Backfill the new contact columns on pre-existing hubspot.contact tables.
    # ADD COLUMN IF NOT EXISTS is idempotent.
    "alter table hubspot.contact add column if not exists property_referring_partner text",
    "alter table hubspot.contact add column if not exists property_lead_origin text",
    "alter table hubspot.contact add column if not exists property_lead_source text",
    "alter table hubspot.contact add column if not exists property_partner_source text",

    """create table if not exists hubspot.deal (
        deal_id text primary key,
        property_dealname text,
        property_pipeline text,
        property_dealstage text,
        property_hs_deal_stage_probability text,
        property_amount text,
        property_amount_in_home_currency text,
        property_createdate text,
        property_closedate text,
        property_hs_closed_won_date text,
        property_hs_is_closed_won text,
        property_hs_is_closed text,
        property_dealtype text,
        property_hubspot_owner_id text,
        property_deal_source text,
        property_referring_partner text,
        property_deal_source_detail text,
        _fivetran_synced timestamptz not null default now(),
        _archived boolean not null default false
    )""",

    # Backfill the new deal columns on pre-existing hubspot.deal tables.
    "alter table hubspot.deal add column if not exists property_deal_source text",
    "alter table hubspot.deal add column if not exists property_referring_partner text",
    "alter table hubspot.deal add column if not exists property_deal_source_detail text",

    """create table if not exists hubspot.deal_stage_history (
        deal_id text not null,
        stage_id text not null,
        entered_at timestamptz not null,
        exited_at timestamptz,
        primary key (deal_id, stage_id, entered_at)
    )""",

    """create table if not exists hubspot.deal_contact (
        deal_id text not null,
        contact_id text not null,
        is_primary boolean not null default false,
        primary key (deal_id, contact_id)
    )""",

    """create table if not exists hubspot.engagement (
        engagement_id text primary key,
        engagement_type text not null,
        engagement_timestamp timestamptz,
        owner_id text,
        engagement_source text,
        email_metadata_direction text,
        email_metadata_from_email text,
        email_metadata_subject text,
        _fivetran_synced timestamptz not null default now(),
        _archived boolean not null default false
    )""",

    """create table if not exists hubspot.engagement_contact (
        engagement_id text not null,
        contact_id text not null,
        primary key (engagement_id, contact_id)
    )""",

    """create table if not exists hubspot.owner (
        owner_id text primary key,
        email text,
        first_name text,
        last_name text,
        team_id text,
        created_at timestamptz,
        archived boolean not null default false
    )""",

    """create table if not exists hubspot.form (
        form_id text primary key,
        name text
    )""",

    """create table if not exists hubspot.contact_form (
        conversion_id text primary key,
        contact_id text,
        form_id text not null,
        submitted_at timestamptz,
        page_url text,
        submission_values jsonb
    )""",

    # Declared as a dbt source but not consumed by any model; create so
    # `dbt source freshness` wouldn't error if it's ever enabled.
    """create table if not exists hubspot.deal_pipeline_stage (
        pipeline_id text not null,
        stage_id text not null,
        label text,
        probability numeric,
        display_order int,
        primary key (pipeline_id, stage_id)
    )""",

    # One-shot migration registry. Each entry runs once.
    """create table if not exists hubspot._migrations (
        migration_id text primary key,
        applied_at timestamptz not null default now()
    )""",

    # Migration: after adding contact/deal partner-attribution columns, force a
    # one-time full rescan so the new columns (referring_partner, deal_source,
    # etc.) get backfilled for every existing row — otherwise the next sync
    # would only refresh recently-modified records. Safe to keep in place:
    # _migrations guards it against running more than once.
    """do $$
    begin
        if not exists (
            select 1 from hubspot._migrations
            where migration_id = '2026_04_backfill_partner_attribution_columns'
        ) then
            delete from hubspot._sync_state where object_name in ('contact', 'deal');
            insert into hubspot._migrations (migration_id)
            values ('2026_04_backfill_partner_attribution_columns');
        end if;
    end $$""",
]


# -----------------------------------------------------------------------------
# HubSpot API client
# -----------------------------------------------------------------------------

class HubSpot:
    def __init__(self, token: str):
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def _req(self, method: str, path: str, **kw) -> dict[str, Any]:
        url = path if path.startswith("http") else f"{API}{path}"
        for attempt in range(6):
            r = self.s.request(method, url, timeout=60, **kw)
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After", "2"))
                log.warning("429 rate-limited, sleeping %.1fs", wait)
                time.sleep(wait)
                continue
            if r.status_code >= 500:
                wait = 2 ** attempt
                log.warning("%d on %s, sleeping %ds", r.status_code, path, wait)
                time.sleep(wait)
                continue
            if r.status_code == 403 and "MISSING_SCOPES" in r.text:
                raise MissingScopes(f"HubSpot {method} {path} 403: {r.text[:500]}")
            if not r.ok:
                raise RuntimeError(f"HubSpot {method} {path} {r.status_code}: {r.text[:500]}")
            return r.json()
        raise RuntimeError(f"HubSpot {method} {path}: exhausted retries")

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._req("GET", path, params=params)

    def post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._req("POST", path, json=body)

    # Paginated list of CRM objects. Supports archived=true and properties list.
    def list_objects(
        self,
        object_name: str,
        properties: list[str] | None = None,
        properties_with_history: list[str] | None = None,
        associations: list[str] | None = None,
        archived: bool = False,
    ) -> Iterator[dict[str, Any]]:
        # HubSpot caps page size at 50 when propertiesWithHistory is requested.
        limit = 50 if properties_with_history else PAGE_SIZE
        params: dict[str, Any] = {"limit": limit, "archived": "true" if archived else "false"}
        if properties:
            params["properties"] = ",".join(properties)
        if properties_with_history:
            params["propertiesWithHistory"] = ",".join(properties_with_history)
        if associations:
            params["associations"] = ",".join(associations)
        after: str | None = None
        while True:
            if after:
                params["after"] = after
            page = self.get(f"/crm/v3/objects/{object_name}", params=params)
            for row in page.get("results", []):
                yield row
            nxt = page.get("paging", {}).get("next")
            if not nxt:
                return
            after = nxt["after"]

    # Search objects modified since `since_ts` (ISO string). Search API caps
    # each page at 100 and total results at 10,000 — if we hit that ceiling we
    # fall back to full scan.
    def search_modified_since(
        self,
        object_name: str,
        properties: list[str],
        since_ts: str,
        properties_with_history: list[str] | None = None,
        associations: list[str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        path = f"/crm/v3/objects/{object_name}/search"
        after: str | None = None
        total_seen = 0
        while True:
            body: dict[str, Any] = {
                "filterGroups": [{
                    "filters": [{
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "GTE",
                        "value": since_ts,
                    }],
                }],
                "sorts": [{"propertyName": "hs_lastmodifieddate", "direction": "ASCENDING"}],
                "properties": properties,
                "limit": PAGE_SIZE,
            }
            if after:
                body["after"] = after
            page = self.post(path, body)
            results = page.get("results", [])
            total_seen += len(results)
            # The search endpoint doesn't expose propertiesWithHistory /
            # associations, so we re-fetch each hit via /objects/{id}.
            for row in results:
                if properties_with_history or associations:
                    params: dict[str, Any] = {}
                    if properties:
                        params["properties"] = ",".join(properties)
                    if properties_with_history:
                        params["propertiesWithHistory"] = ",".join(properties_with_history)
                    if associations:
                        params["associations"] = ",".join(associations)
                    full = self.get(f"/crm/v3/objects/{object_name}/{row['id']}", params=params)
                    yield full
                else:
                    yield row
            nxt = page.get("paging", {}).get("next")
            if not nxt:
                return
            if total_seen >= 9900:
                # Approaching search ceiling; caller should fall back to a full
                # scan. Signal by raising.
                raise SearchCeilingHit()
            after = nxt["after"]


class SearchCeilingHit(Exception):
    """Raised when an incremental search would exceed HubSpot's 10k cap."""


class MissingScopes(Exception):
    """Raised when the HubSpot private app lacks scopes for an object type.
    Callers that treat the object as optional can catch this and skip."""


# -----------------------------------------------------------------------------
# Postgres helpers
# -----------------------------------------------------------------------------

def _conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def init_schema(conn) -> None:
    with conn.cursor() as cur:
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt)
    conn.commit()


def get_last_sync(conn, object_name: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            "select last_sync_at from hubspot._sync_state where object_name = %s",
            (object_name,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def set_last_sync(conn, object_name: str, ts: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into hubspot._sync_state (object_name, last_sync_at)
            values (%s, %s)
            on conflict (object_name) do update set last_sync_at = excluded.last_sync_at
            """,
            (object_name, ts),
        )
    conn.commit()


def upsert(conn, table: str, pk_cols: list[str], rows: list[dict[str, Any]]) -> int:
    """Bulk INSERT ... ON CONFLICT DO UPDATE on the given PK."""
    if not rows:
        return 0
    cols = list(rows[0].keys())
    col_sql = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    update_cols = [c for c in cols if c not in pk_cols]
    set_sql = ", ".join(f"{c} = excluded.{c}" for c in update_cols) or f"{pk_cols[0]} = excluded.{pk_cols[0]}"
    pk_sql = ", ".join(pk_cols)
    sql = (
        f"insert into hubspot.{table} ({col_sql}) values ({placeholders}) "
        f"on conflict ({pk_sql}) do update set {set_sql}"
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur, sql, [[r.get(c) for c in cols] for r in rows], page_size=500
        )
    conn.commit()
    return len(rows)


# -----------------------------------------------------------------------------
# Timestamp parsing (HubSpot returns epoch ms as string, or ISO)
# -----------------------------------------------------------------------------

def to_ts(val: Any) -> datetime | None:
    if val in (None, ""):
        return None
    # Epoch milliseconds (string or int)
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
    s = str(val)
    if s.isdigit():
        return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
    # ISO with or without trailing Z
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


# -----------------------------------------------------------------------------
# Sync: contacts
# -----------------------------------------------------------------------------

def _contact_row(obj: dict[str, Any], archived: bool) -> dict[str, Any]:
    props = obj.get("properties", {})
    row: dict[str, Any] = {"id": obj["id"], "_archived": archived}
    for p in CONTACT_PROPS:
        if p == "hs_lastmodifieddate":
            continue
        row[f"property_{p}"] = props.get(p)
    row["_fivetran_synced"] = datetime.now(tz=timezone.utc)
    return row


def sync_contacts(hs: HubSpot, conn, since: datetime | None) -> int:
    total = 0
    try:
        it: Iterable[dict[str, Any]]
        if since is None:
            it = hs.list_objects("contacts", properties=CONTACT_PROPS)
        else:
            it = hs.search_modified_since(
                "contacts", CONTACT_PROPS, since.isoformat(timespec="seconds")
            )
        batch: list[dict[str, Any]] = []
        for obj in it:
            batch.append(_contact_row(obj, archived=False))
            if len(batch) >= 500:
                total += upsert(conn, "contact", ["id"], batch)
                batch = []
        total += upsert(conn, "contact", ["id"], batch)
    except SearchCeilingHit:
        log.warning("contact search hit ceiling — re-running as full scan")
        return sync_contacts(hs, conn, since=None)

    # Archived pass — flag any archived contacts.
    archived_batch: list[dict[str, Any]] = []
    for obj in hs.list_objects("contacts", properties=CONTACT_PROPS, archived=True):
        archived_batch.append(_contact_row(obj, archived=True))
        if len(archived_batch) >= 500:
            total += upsert(conn, "contact", ["id"], archived_batch)
            archived_batch = []
    total += upsert(conn, "contact", ["id"], archived_batch)
    return total


# -----------------------------------------------------------------------------
# Sync: deals (also emits deal_stage_history and deal_contact)
# -----------------------------------------------------------------------------

def _deal_row(obj: dict[str, Any], archived: bool) -> dict[str, Any]:
    props = obj.get("properties", {})
    row: dict[str, Any] = {"deal_id": obj["id"], "_archived": archived}
    for p in DEAL_PROPS:
        if p == "hs_lastmodifieddate":
            continue
        row[f"property_{p}"] = props.get(p)
    row["_fivetran_synced"] = datetime.now(tz=timezone.utc)
    return row


def _deal_stage_history(deal_obj: dict[str, Any]) -> list[dict[str, Any]]:
    # v3 returns `propertiesWithHistory.dealstage` as list ordered newest-first.
    hist = (deal_obj.get("propertiesWithHistory") or {}).get("dealstage") or []
    if not hist:
        return []
    deal_id = deal_obj["id"]
    # Sort oldest-first so we can compute `exited_at` as the next entry's timestamp.
    entries = sorted(
        (h for h in hist if h.get("value") and h.get("timestamp")),
        key=lambda h: h["timestamp"],
    )
    rows: list[dict[str, Any]] = []
    for i, h in enumerate(entries):
        entered = to_ts(h["timestamp"])
        if entered is None:
            continue
        exited = to_ts(entries[i + 1]["timestamp"]) if i + 1 < len(entries) else None
        rows.append({
            "deal_id": deal_id,
            "stage_id": h["value"],
            "entered_at": entered,
            "exited_at": exited,
        })
    return rows


def _deal_contacts(deal_obj: dict[str, Any]) -> list[dict[str, Any]]:
    assoc = (deal_obj.get("associations") or {}).get("contacts") or {}
    results = assoc.get("results") or []
    out: list[dict[str, Any]] = []
    # Heuristic: first associated contact = primary. HubSpot does not expose
    # is_primary on associations in a machine-readable way via v3; downstream
    # `int_deal_primary_contact` also falls back to earliest-created contact,
    # so even if every row has is_primary=false the join still works.
    for i, r in enumerate(results):
        out.append({
            "deal_id": deal_obj["id"],
            "contact_id": r["id"],
            "is_primary": i == 0,
        })
    return out


def sync_deals(hs: HubSpot, conn, since: datetime | None) -> int:
    total = 0
    def _process(iterator: Iterable[dict[str, Any]], archived: bool) -> int:
        n = 0
        batch: list[dict[str, Any]] = []
        hist_batch: list[dict[str, Any]] = []
        dc_batch: list[dict[str, Any]] = []
        for obj in iterator:
            batch.append(_deal_row(obj, archived=archived))
            hist_batch.extend(_deal_stage_history(obj))
            dc_batch.extend(_deal_contacts(obj))
            if len(batch) >= 500:
                n += upsert(conn, "deal", ["deal_id"], batch)
                upsert(conn, "deal_stage_history",
                       ["deal_id", "stage_id", "entered_at"], hist_batch)
                upsert(conn, "deal_contact", ["deal_id", "contact_id"], dc_batch)
                batch, hist_batch, dc_batch = [], [], []
        n += upsert(conn, "deal", ["deal_id"], batch)
        upsert(conn, "deal_stage_history",
               ["deal_id", "stage_id", "entered_at"], hist_batch)
        upsert(conn, "deal_contact", ["deal_id", "contact_id"], dc_batch)
        return n

    try:
        if since is None:
            it = hs.list_objects(
                "deals", properties=DEAL_PROPS,
                properties_with_history=["dealstage"],
                associations=["contacts"],
            )
        else:
            it = hs.search_modified_since(
                "deals", DEAL_PROPS, since.isoformat(timespec="seconds"),
                properties_with_history=["dealstage"],
                associations=["contacts"],
            )
        total += _process(it, archived=False)
    except SearchCeilingHit:
        log.warning("deal search hit ceiling — re-running as full scan")
        return sync_deals(hs, conn, since=None)

    archived_it = hs.list_objects(
        "deals", properties=DEAL_PROPS,
        properties_with_history=["dealstage"],
        associations=["contacts"],
        archived=True,
    )
    total += _process(archived_it, archived=True)
    return total


# -----------------------------------------------------------------------------
# Sync: owners
# -----------------------------------------------------------------------------

def sync_owners(hs: HubSpot, conn) -> int:
    rows: list[dict[str, Any]] = []
    after: str | None = None
    while True:
        params: dict[str, Any] = {"limit": PAGE_SIZE}
        if after:
            params["after"] = after
        page = hs.get("/crm/v3/owners", params=params)
        for o in page.get("results", []):
            rows.append({
                "owner_id": str(o.get("id")),
                "email": o.get("email"),
                "first_name": o.get("firstName"),
                "last_name": o.get("lastName"),
                "team_id": str(o["teamId"]) if o.get("teamId") else None,
                "created_at": to_ts(o.get("createdAt")),
                "archived": bool(o.get("archived", False)),
            })
        nxt = page.get("paging", {}).get("next")
        if not nxt:
            break
        after = nxt["after"]
    return upsert(conn, "owner", ["owner_id"], rows)


# -----------------------------------------------------------------------------
# Sync: engagements (emails / calls / meetings / notes / tasks)
# -----------------------------------------------------------------------------

def _engagement_row(obj: dict[str, Any], etype: str, archived: bool) -> dict[str, Any]:
    props = obj.get("properties", {})
    row: dict[str, Any] = {
        "engagement_id": obj["id"],
        "engagement_type": etype,
        "engagement_timestamp": to_ts(props.get("hs_timestamp") or props.get("hs_createdate")),
        "owner_id": props.get("hubspot_owner_id"),
        "engagement_source": etype,
        "email_metadata_direction": props.get("hs_email_direction"),
        "email_metadata_from_email": props.get("hs_email_from_email"),
        "email_metadata_subject": props.get("hs_email_subject"),
        "_fivetran_synced": datetime.now(tz=timezone.utc),
        "_archived": archived,
    }
    return row


def _engagement_contacts(obj: dict[str, Any]) -> list[dict[str, Any]]:
    assoc = (obj.get("associations") or {}).get("contacts") or {}
    results = assoc.get("results") or []
    return [{"engagement_id": obj["id"], "contact_id": r["id"]} for r in results]


def sync_engagements(hs: HubSpot, conn, since: datetime | None) -> int:
    total = 0
    for obj_name, etype, props in ENGAGEMENT_OBJECTS:
        log.info("engagements: %s", obj_name)
        try:
            if since is None:
                it = hs.list_objects(obj_name, properties=props, associations=["contacts"])
            else:
                it = hs.search_modified_since(
                    obj_name, props, since.isoformat(timespec="seconds"),
                    associations=["contacts"],
                )
            batch: list[dict[str, Any]] = []
            assoc_batch: list[dict[str, Any]] = []
            for obj in it:
                batch.append(_engagement_row(obj, etype, archived=False))
                assoc_batch.extend(_engagement_contacts(obj))
                if len(batch) >= 500:
                    total += upsert(conn, "engagement", ["engagement_id"], batch)
                    upsert(conn, "engagement_contact",
                           ["engagement_id", "contact_id"], assoc_batch)
                    batch, assoc_batch = [], []
            total += upsert(conn, "engagement", ["engagement_id"], batch)
            upsert(conn, "engagement_contact",
                   ["engagement_id", "contact_id"], assoc_batch)
        except MissingScopes as e:
            log.warning("skipping engagements/%s — missing scopes: %s", obj_name, e)
            continue
        except SearchCeilingHit:
            log.warning("%s search hit ceiling — doing full scan for this type", obj_name)
            it = hs.list_objects(obj_name, properties=props, associations=["contacts"])
            batch, assoc_batch = [], []
            for obj in it:
                batch.append(_engagement_row(obj, etype, archived=False))
                assoc_batch.extend(_engagement_contacts(obj))
                if len(batch) >= 500:
                    total += upsert(conn, "engagement", ["engagement_id"], batch)
                    upsert(conn, "engagement_contact",
                           ["engagement_id", "contact_id"], assoc_batch)
                    batch, assoc_batch = [], []
            total += upsert(conn, "engagement", ["engagement_id"], batch)
            upsert(conn, "engagement_contact",
                   ["engagement_id", "contact_id"], assoc_batch)
    return total


# -----------------------------------------------------------------------------
# Sync: forms + submissions
# -----------------------------------------------------------------------------

def sync_forms(hs: HubSpot, conn) -> list[dict[str, Any]]:
    """Returns the list of form dicts for use by sync_form_submissions."""
    rows: list[dict[str, Any]] = []
    forms: list[dict[str, Any]] = []
    after: str | None = None
    while True:
        params: dict[str, Any] = {"limit": PAGE_SIZE}
        if after:
            params["after"] = after
        page = hs.get("/marketing/v3/forms", params=params)
        for f in page.get("results", []):
            fid = f.get("id") or f.get("guid")
            if not fid:
                continue
            forms.append(f)
            rows.append({"form_id": fid, "name": f.get("name")})
        nxt = page.get("paging", {}).get("next")
        if not nxt:
            break
        after = nxt["after"]
    upsert(conn, "form", ["form_id"], rows)
    return forms


def sync_form_submissions(hs: HubSpot, conn, forms: list[dict[str, Any]]) -> int:
    """For each form, fetch submissions and upsert into contact_form.

    HubSpot's form-submissions endpoint doesn't return contactId directly, so
    we resolve contact_id by email from the already-synced hubspot.contact
    table after the insert (cheap single SQL statement)."""
    total = 0
    for f in forms:
        fid = f.get("id") or f.get("guid")
        after: str | None = None
        while True:
            params: dict[str, Any] = {"limit": 50}
            if after:
                params["after"] = after
            try:
                page = hs.get(f"/form-integrations/v1/submissions/forms/{fid}", params=params)
            except RuntimeError as e:
                # Some forms are legacy/archived and return 404; skip them.
                log.warning("form %s submissions skipped: %s", fid, e)
                break
            rows: list[dict[str, Any]] = []
            for s in page.get("results", []):
                # values is a list of {name, value, objectTypeId?}. Flatten to a
                # JSON object keyed by field name so the staging model's
                # json_value(submission_values, 'field') macro works.
                values = s.get("values") or []
                flat = {v.get("name"): v.get("value") for v in values if v.get("name")}
                submitted_at = to_ts(s.get("submittedAt"))
                # Per-submission conversion id: HubSpot returns none in v1; use
                # form_id + submittedAt + email as a stable synthetic key.
                email = flat.get("email")
                conv_id = (
                    s.get("conversionId")
                    or f"{fid}:{int(submitted_at.timestamp()) if submitted_at else 0}:{email or ''}"
                )
                rows.append({
                    "conversion_id": conv_id,
                    "contact_id": None,   # filled in by join below
                    "form_id": fid,
                    "submitted_at": submitted_at,
                    "page_url": s.get("pageUrl"),
                    "submission_values": psycopg2.extras.Json(flat),
                })
            total += upsert(
                conn, "contact_form",
                ["conversion_id"], rows,
            )
            nxt = page.get("paging", {}).get("next")
            if not nxt:
                break
            after = nxt["after"]

    # Resolve contact_id by email after the fact. Any submission whose email
    # matches an existing contact gets linked.
    with conn.cursor() as cur:
        cur.execute("""
            update hubspot.contact_form cf
               set contact_id = c.id
              from hubspot.contact c
             where cf.contact_id is null
               and c.property_email is not null
               and lower(c.property_email) = lower(
                   coalesce(cf.submission_values->>'email', '')
               )
               and cf.submission_values->>'email' is not null
               and cf.submission_values->>'email' <> ''
        """)
    conn.commit()
    return total


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    token = os.environ.get("HUBSPOT_ACCESS_TOKEN")
    if not token:
        print("HUBSPOT_ACCESS_TOKEN not set", file=sys.stderr)
        return 2
    if not os.environ.get("DATABASE_URL"):
        print("DATABASE_URL not set", file=sys.stderr)
        return 2

    hs = HubSpot(token)
    run_started = datetime.now(tz=timezone.utc)

    with _conn() as conn:
        init_schema(conn)

        # Owners — small, always full scan.
        n = sync_owners(hs, conn)
        log.info("owners: %d rows", n)

        # Contacts.
        since = get_last_sync(conn, "contact")
        n = sync_contacts(hs, conn, since)
        set_last_sync(conn, "contact", run_started)
        log.info("contacts: %d rows (since=%s)", n, since)

        # Deals (plus derived deal_stage_history, deal_contact).
        since = get_last_sync(conn, "deal")
        n = sync_deals(hs, conn, since)
        set_last_sync(conn, "deal", run_started)
        log.info("deals: %d rows (since=%s)", n, since)

        # Engagements.
        since = get_last_sync(conn, "engagement")
        n = sync_engagements(hs, conn, since)
        set_last_sync(conn, "engagement", run_started)
        log.info("engagements: %d rows (since=%s)", n, since)

        # Forms + submissions — always full scan. Forms list is small and
        # submission volume is typically modest. Skipped if the private app
        # lacks the `forms` / `forms-uploaded-files` scopes.
        try:
            forms = sync_forms(hs, conn)
            log.info("forms: %d", len(forms))
            n = sync_form_submissions(hs, conn, forms)
            log.info("form submissions: %d rows", n)
        except MissingScopes as e:
            log.warning("skipping forms — missing scopes: %s", e)

    return 0


if __name__ == "__main__":
    sys.exit(main())
