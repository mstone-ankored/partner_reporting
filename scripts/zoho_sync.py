"""Push partner_reporting marts from the warehouse to Zoho Analytics.

Run as the second step of the refresh pipeline (after `dbt build`):

    python scripts/zoho_sync.py

Flow per table:
  1. Read rows from the warehouse via SQLAlchemy.
  2. Stream them as CSV into Zoho Analytics' Bulk Import API (v2).
  3. Use TRUNCATEADD for aggregate/dim tables (cheap, always correct) and
     UPDATEADD (upsert) for the large fact tables partner_leads / partner_deals
     so ongoing syncs don't re-upload the full history.

All auth is OAuth 2.0; the refresh token is long-lived and kept in
ZOHO_REFRESH_TOKEN, never committed.

Env vars required:
  WAREHOUSE_URL              SQLAlchemy URL, e.g.
                             bigquery://my-project/partner_reporting_marts
                             snowflake://user:pass@account/db/schema
                             postgresql://user:pass@host/db
  WAREHOUSE_MART_SCHEMA      Schema that holds the mart tables (default: partner_reporting_marts)

  ZOHO_REGION                com | eu | in | com.au | jp (default: com)
  ZOHO_CLIENT_ID             OAuth client id from Zoho API Console
  ZOHO_CLIENT_SECRET         OAuth client secret
  ZOHO_REFRESH_TOKEN         Long-lived refresh token
  ZANALYTICS_ORGID           Zoho Analytics org id
  ZOHO_WORKSPACE_ID          Target workspace id
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("zoho_sync")


# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

ZOHO_REGION = os.environ.get("ZOHO_REGION", "com")
ZOHO_ACCOUNTS_URL = f"https://accounts.zoho.{ZOHO_REGION}/oauth/v2/token"
ZOHO_API_BASE = f"https://analyticsapi.zoho.{ZOHO_REGION}/restapi/v2"

DEFAULT_SCHEMA = os.environ.get("WAREHOUSE_MART_SCHEMA", "partner_reporting_marts")


@dataclass
class TableSync:
    table_name: str
    import_type: str = "TRUNCATEADD"   # or "UPDATEADD" (upsert)
    match_columns: list[str] = field(default_factory=list)

    @property
    def needs_match_cols(self) -> bool:
        return self.import_type == "UPDATEADD"


# Order matters: sync dims first so downstream dashboards don't see partial state.
TABLES: list[TableSync] = [
    TableSync("partner_penetration",                "TRUNCATEADD"),
    TableSync("partner_rankings",                   "TRUNCATEADD"),
    TableSync("partner_rep_performance",            "TRUNCATEADD"),
    TableSync("partner_funnel_stage_conversion",    "TRUNCATEADD"),
    TableSync("partner_deal_stage_durations",       "TRUNCATEADD"),
    TableSync("partner_lead_cohorts",               "TRUNCATEADD"),
    TableSync("partner_summary",                    "TRUNCATEADD"),
    TableSync("partner_leads",                      "UPDATEADD", ["contact_id"]),
    TableSync("partner_deals",                      "UPDATEADD", ["deal_id"]),
]


# --------------------------------------------------------------------------- #
# Zoho auth + API helpers                                                     #
# --------------------------------------------------------------------------- #

def require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"Missing required environment variable: {name}")
    return val


def refresh_access_token() -> str:
    resp = requests.post(
        ZOHO_ACCOUNTS_URL,
        params={
            "refresh_token": require_env("ZOHO_REFRESH_TOKEN"),
            "client_id":     require_env("ZOHO_CLIENT_ID"),
            "client_secret": require_env("ZOHO_CLIENT_SECRET"),
            "grant_type":    "refresh_token",
        },
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if "access_token" not in payload:
        raise RuntimeError(f"Zoho did not return an access_token: {payload}")
    return payload["access_token"]


def zoho_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": require_env("ZANALYTICS_ORGID"),
    }


def list_views(access_token: str, workspace_id: str) -> dict[str, str]:
    """Return a dict of {view_name_lower: view_id} for all views in the workspace."""
    resp = requests.get(
        f"{ZOHO_API_BASE}/workspaces/{workspace_id}/views",
        headers=zoho_headers(access_token),
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    views = body.get("data", {}).get("views", [])
    return {v["viewName"].lower(): v["viewId"] for v in views}


def import_to_view(
    access_token: str,
    workspace_id: str,
    view_id: str,
    csv_data: str,
    import_type: str,
    match_columns: list[str],
) -> dict[str, Any]:
    config: dict[str, Any] = {
        "importType": import_type,
        "fileType": "csv",
        "autoIdentify": "true",
        "onError": "SETCOLUMNEMPTY",
    }
    if import_type == "UPDATEADD" and match_columns:
        config["matchingColumns"] = ",".join(match_columns)

    resp = requests.post(
        f"{ZOHO_API_BASE}/workspaces/{workspace_id}/views/{view_id}/data",
        headers=zoho_headers(access_token),
        files={"FILE": ("data.csv", csv_data, "text/csv")},
        data={"CONFIG": json.dumps(config)},
        timeout=600,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Zoho import failed ({resp.status_code}): {resp.text}")
    return resp.json()


# --------------------------------------------------------------------------- #
# Warehouse read                                                              #
# --------------------------------------------------------------------------- #

def pull_table_as_csv(engine: Engine, schema: str, table: str) -> str | None:
    """Read every row of schema.table and return a CSV string, or None if empty.

    Streams into an in-memory buffer. For very large fact tables you should
    instead page the read (LIMIT/OFFSET or watermark) and POST in chunks;
    see docs/zoho_analytics_setup.md for details. Zoho's import accepts up
    to 2GB per request, which covers most HubSpot-scale deployments.
    """
    buf = io.StringIO()
    with engine.connect() as conn:
        result = conn.execute(text(f"select * from {schema}.{table}"))
        rows = result.mappings().all()
        if not rows:
            return None

        field_names = list(rows[0].keys())
        writer = csv.DictWriter(buf, fieldnames=field_names, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if v is None else v) for k, v in row.items()})

    return buf.getvalue()


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #

def main() -> int:
    warehouse_url = require_env("WAREHOUSE_URL")
    workspace_id = require_env("ZOHO_WORKSPACE_ID")

    engine = create_engine(warehouse_url)
    access_token = refresh_access_token()
    log.info("Acquired Zoho access token for region %s", ZOHO_REGION)

    view_map = list_views(access_token, workspace_id)
    log.info("Zoho workspace %s has %d views", workspace_id, len(view_map))

    missing: list[str] = []
    failed: list[str] = []

    for sync in TABLES:
        view_id = view_map.get(sync.table_name.lower())
        if not view_id:
            missing.append(sync.table_name)
            log.warning(
                "Zoho view '%s' does not exist — run scripts/bootstrap_zoho_workspace.py",
                sync.table_name,
            )
            continue

        try:
            csv_data = pull_table_as_csv(engine, DEFAULT_SCHEMA, sync.table_name)
            if csv_data is None:
                log.info("%s is empty in the warehouse — skipping", sync.table_name)
                continue

            result = import_to_view(
                access_token, workspace_id, view_id,
                csv_data, sync.import_type, sync.match_columns,
            )
            job_id = result.get("data", {}).get("jobId", "<n/a>")
            log.info(
                "Imported %s via %s (job=%s)",
                sync.table_name, sync.import_type, job_id,
            )
        except Exception as exc:
            log.exception("Failed to sync %s: %s", sync.table_name, exc)
            failed.append(sync.table_name)

    if missing:
        log.error("Missing Zoho views: %s", missing)
    if failed:
        log.error("Failed syncs: %s", failed)
    if missing or failed:
        return 1

    log.info("Zoho sync complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
