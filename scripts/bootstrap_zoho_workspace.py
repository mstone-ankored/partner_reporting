"""One-time setup: create Zoho Analytics tables that mirror the warehouse marts.

Reads the column list of each mart table from the warehouse's information
schema, maps types to Zoho Analytics data types, and POSTs a CreateTable call
for each table that does not already exist.

Idempotent: re-running skips tables that already exist. To recreate a table
(e.g. after adding columns), delete it in the Zoho UI and re-run.

    python scripts/bootstrap_zoho_workspace.py

Uses the same env vars as zoho_sync.py.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

import requests
from sqlalchemy import create_engine, inspect

from zoho_sync import (  # type: ignore[import-not-found]
    DEFAULT_SCHEMA,
    TABLES,
    ZOHO_API_BASE,
    list_views,
    refresh_access_token,
    require_env,
    zoho_headers,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bootstrap_zoho")


# Map SQLAlchemy-reflected type strings to Zoho Analytics column data types.
# Reference: https://www.zoho.com/analytics/api/v2/ column-datatype section.
def sql_type_to_zoho(sql_type: Any) -> str:
    t = str(sql_type).upper()
    if "BOOL" in t:
        return "BOOLEAN"
    if "DATE" in t and "TIME" not in t:
        return "DATE"
    if "TIMESTAMP" in t or "DATETIME" in t:
        return "DATE"
    if any(kw in t for kw in ("NUMERIC", "DECIMAL", "FLOAT", "DOUBLE", "REAL")):
        return "DECIMAL_NUMBER"
    if any(kw in t for kw in ("INT", "BIGINT", "SMALLINT")):
        return "NUMBER"
    return "PLAIN"


def reflect_columns(engine, schema: str, table: str) -> list[dict[str, str]]:
    inspector = inspect(engine)
    raw_columns = inspector.get_columns(table, schema=schema)
    if not raw_columns:
        raise RuntimeError(
            f"No columns reflected for {schema}.{table}. "
            "Did you run `dbt build`?"
        )
    return [
        {"columnName": col["name"], "dataType": sql_type_to_zoho(col["type"])}
        for col in raw_columns
    ]


def create_view(
    access_token: str,
    workspace_id: str,
    view_name: str,
    columns: list[dict[str, str]],
    folder_name: str = "Partner Reporting",
) -> dict[str, Any]:
    config = {
        "viewName": view_name,
        "folderName": folder_name,
        "columns": columns,
    }
    resp = requests.post(
        f"{ZOHO_API_BASE}/workspaces/{workspace_id}/views",
        headers=zoho_headers(access_token),
        data={"CONFIG": json.dumps(config)},
        timeout=60,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Create view failed ({resp.status_code}): {resp.text}")
    return resp.json()


def main() -> int:
    warehouse_url = require_env("WAREHOUSE_URL")
    workspace_id = require_env("ZOHO_WORKSPACE_ID")

    engine = create_engine(warehouse_url)
    access_token = refresh_access_token()

    existing = list_views(access_token, workspace_id)
    log.info("Found %d existing views in workspace %s", len(existing), workspace_id)

    created = 0
    for sync in TABLES:
        if sync.table_name.lower() in existing:
            log.info("View '%s' already exists — skipping", sync.table_name)
            continue

        try:
            columns = reflect_columns(engine, DEFAULT_SCHEMA, sync.table_name)
        except Exception as exc:
            log.error("Could not reflect %s: %s", sync.table_name, exc)
            continue

        log.info("Creating view '%s' with %d columns", sync.table_name, len(columns))
        create_view(access_token, workspace_id, sync.table_name, columns)
        created += 1

    log.info("Bootstrap complete — %d new views created", created)
    return 0


if __name__ == "__main__":
    sys.exit(main())
