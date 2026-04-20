"""
Push selected partner-reporting mart rows into Notion databases.

Config lives in Neon (table `partner_reporting_app.notion_sync_targets`), which
is populated by the web app's /settings/notion page. Each row says:
  * which mart table to read (e.g. partner_rankings)
  * which Notion database to upsert into
  * which columns to push and how to name them as Notion properties
  * optional row-level filters (e.g. period_type = 'all_time')

For each target we:
  1. Read rows from the mart table (with optional filters).
  2. Query the Notion database to find existing pages by title.
  3. Create or update each row (upsert by the target's title column).

Requires env:
  DATABASE_URL        Neon pooled URL
  NOTION_API_KEY      Notion integration token (Bearer)

Exit status: 0 on success across all enabled targets; non-zero if any target
fails (but other targets still run).
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extras
import requests

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


# -----------------------------------------------------------------------------
# Config loading (Neon)
# -----------------------------------------------------------------------------

@dataclass
class Target:
    id: str
    source_table: str
    notion_database_id: str
    filter_json: dict[str, Any]
    column_map_json: dict[str, str]


def _conn():
    url = os.environ["DATABASE_URL"]
    return psycopg2.connect(url)


def load_targets(conn) -> list[Target]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select id::text, source_table, notion_database_id,
                   filter_json, column_map_json
            from partner_reporting_app.notion_sync_targets
            where enabled = true
            order by source_table asc
            """
        )
        return [Target(**row) for row in cur.fetchall()]


def mark_result(conn, target_id: str, ok: bool, message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update partner_reporting_app.notion_sync_targets
               set last_synced_at = now(),
                   last_sync_status = %s,
                   last_sync_message = %s
             where id = %s
            """,
            ("ok" if ok else "error", message[:500], target_id),
        )
    conn.commit()


# -----------------------------------------------------------------------------
# Source reading (mart tables)
# -----------------------------------------------------------------------------

MART_SCHEMA = os.environ.get("MART_SCHEMA", "partner_reporting")

# Each source defines: title column, allowed filter keys. This must stay in sync
# with web/lib/notion-targets.ts — the UI offers the same set.
SOURCES: dict[str, dict[str, Any]] = {
    "partner_rankings": {
        "title_column": "partner_name",
        "filter_keys": [],
    },
    "partner_summary": {
        "title_column": "partner_name",
        "filter_keys": ["period_type"],
        "default_filters": {"period_type": "all_time"},
    },
    "partner_penetration": {
        "title_column": "partner_name",
        "filter_keys": [],
    },
}


def read_source(conn, target: Target) -> tuple[list[dict[str, Any]], str]:
    spec = SOURCES.get(target.source_table)
    if not spec:
        raise ValueError(f"Unknown source table: {target.source_table}")

    # Restrict selected columns to the user's column_map, plus the title column.
    selected = list(target.column_map_json.keys())
    if spec["title_column"] not in selected:
        selected.insert(0, spec["title_column"])
    # Defense: identifier whitelisting — only a-z0-9_ allowed.
    for c in selected:
        if not c.replace("_", "").isalnum():
            raise ValueError(f"Bad column name: {c}")

    cols = ", ".join(selected)
    filters = dict(spec.get("default_filters", {}))
    filters.update(target.filter_json or {})
    where_clauses = []
    params: list[Any] = []
    for k, v in filters.items():
        if k not in spec.get("filter_keys", []):
            continue
        if v in (None, "", []):
            continue
        where_clauses.append(f"{k} = %s")
        params.append(v)
    where_sql = (" where " + " and ".join(where_clauses)) if where_clauses else ""
    sql = f"select {cols} from {MART_SCHEMA}.{target.source_table}{where_sql}"

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
    return rows, spec["title_column"]


# -----------------------------------------------------------------------------
# Notion API
# -----------------------------------------------------------------------------

class NotionClient:
    def __init__(self, token: str):
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": f"Bearer {token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        })

    def _req(self, method: str, path: str, **kw) -> dict[str, Any]:
        url = f"{NOTION_API}{path}"
        for attempt in range(5):
            r = self.s.request(method, url, timeout=30, **kw)
            if r.status_code == 429:
                time.sleep(float(r.headers.get("Retry-After", "1")))
                continue
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            if not r.ok:
                raise RuntimeError(f"Notion {method} {path} {r.status_code}: {r.text}")
            return r.json()
        raise RuntimeError(f"Notion {method} {path}: exhausted retries")

    def get_database(self, db_id: str) -> dict[str, Any]:
        return self._req("GET", f"/databases/{db_id}")

    def query_all(self, db_id: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        payload: dict[str, Any] = {"page_size": 100}
        while True:
            r = self._req("POST", f"/databases/{db_id}/query", json=payload)
            out.extend(r.get("results", []))
            if not r.get("has_more"):
                return out
            payload["start_cursor"] = r["next_cursor"]

    def create_page(self, db_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        return self._req(
            "POST",
            "/pages",
            json={"parent": {"database_id": db_id}, "properties": properties},
        )

    def update_page(self, page_id: str, properties: dict[str, Any]) -> dict[str, Any]:
        return self._req("PATCH", f"/pages/{page_id}", json={"properties": properties})


# -----------------------------------------------------------------------------
# Value → Notion property conversion
# -----------------------------------------------------------------------------

def _rich_text(s: str) -> dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": s[:2000]}}]}


def _title(s: str) -> dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": s[:2000]}}]}


def _coerce(value: Any, notion_prop_type: str) -> dict[str, Any]:
    if value is None:
        if notion_prop_type == "title":
            return {"title": []}
        return {notion_prop_type: None}
    if isinstance(value, Decimal):
        value = float(value)
    if notion_prop_type == "title":
        return _title(str(value))
    if notion_prop_type == "rich_text":
        return _rich_text(str(value))
    if notion_prop_type == "number":
        try:
            return {"number": float(value)}
        except (TypeError, ValueError):
            return {"number": None}
    if notion_prop_type == "checkbox":
        return {"checkbox": bool(value)}
    if notion_prop_type == "date":
        if isinstance(value, (date, datetime)):
            return {"date": {"start": value.isoformat()}}
        return {"date": {"start": str(value)}}
    if notion_prop_type == "select":
        return {"select": {"name": str(value)[:100]}}
    return _rich_text(str(value))


def build_properties(
    row: dict[str, Any],
    column_map: dict[str, str],
    title_column: str,
    notion_schema: dict[str, Any],
) -> dict[str, Any]:
    props: dict[str, Any] = {}
    notion_props = notion_schema.get("properties", {})

    # Title first.
    title_notion_name = column_map.get(title_column)
    if title_notion_name and title_notion_name in notion_props:
        if notion_props[title_notion_name]["type"] == "title":
            props[title_notion_name] = _coerce(row.get(title_column), "title")

    # Map remaining columns.
    for src_col, notion_name in column_map.items():
        if src_col == title_column:
            continue
        if notion_name not in notion_props:
            continue  # user named a property that doesn't exist in the DB; skip silently
        prop_type = notion_props[notion_name]["type"]
        props[notion_name] = _coerce(row.get(src_col), prop_type)
    return props


def find_title_property_name(notion_schema: dict[str, Any]) -> str:
    for name, p in notion_schema.get("properties", {}).items():
        if p.get("type") == "title":
            return name
    raise RuntimeError("Notion database has no title property")


# -----------------------------------------------------------------------------
# Sync one target
# -----------------------------------------------------------------------------

def sync_target(conn, notion: NotionClient, target: Target) -> str:
    rows, title_column = read_source(conn, target)
    schema = notion.get_database(target.notion_database_id)
    title_prop = find_title_property_name(schema)

    # Build an index of existing Notion pages by their title text.
    existing = notion.query_all(target.notion_database_id)
    by_title: dict[str, str] = {}
    for page in existing:
        title_cell = page.get("properties", {}).get(title_prop, {}).get("title", [])
        text = "".join(t.get("plain_text", "") for t in title_cell).strip()
        if text:
            by_title[text] = page["id"]

    # Ensure column_map has an entry for the title column pointing to the
    # Notion title property, even if the user didn't include it explicitly.
    column_map = dict(target.column_map_json)
    column_map.setdefault(title_column, title_prop)

    created = 0
    updated = 0
    for row in rows:
        title_val = row.get(title_column)
        if title_val is None:
            continue
        props = build_properties(row, column_map, title_column, schema)
        title_text = str(title_val).strip()
        page_id = by_title.get(title_text)
        if page_id:
            notion.update_page(page_id, props)
            updated += 1
        else:
            notion.create_page(target.notion_database_id, props)
            created += 1

    return f"{len(rows)} rows: {created} created, {updated} updated"


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    token = os.environ.get("NOTION_API_KEY")
    if not token:
        print("NOTION_API_KEY not set — skipping Notion sync", file=sys.stderr)
        return 0
    if not os.environ.get("DATABASE_URL"):
        print("DATABASE_URL is required", file=sys.stderr)
        return 2

    notion = NotionClient(token)
    failures = 0
    with _conn() as conn:
        targets = load_targets(conn)
        if not targets:
            print("No enabled notion sync targets; nothing to do.")
            return 0
        for t in targets:
            try:
                msg = sync_target(conn, notion, t)
                mark_result(conn, t.id, True, msg)
                print(f"[ok]  {t.source_table} → {t.notion_database_id}: {msg}")
            except Exception as e:  # noqa: BLE001
                failures += 1
                mark_result(conn, t.id, False, f"{type(e).__name__}: {e}")
                print(
                    f"[err] {t.source_table} → {t.notion_database_id}: {e}",
                    file=sys.stderr,
                )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
