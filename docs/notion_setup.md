# Notion sync setup

The scheduled refresh (`.github/workflows/refresh.yml`) runs
`scripts/notion_sync.py`, which reads targets from the app's
`partner_reporting_app.notion_sync_targets` table and upserts selected rows
into Notion databases.

Everything in the web dashboard stays live on Neon; Notion is the "executive
summary" surface.

## 1. Create a Notion integration

1. Go to <https://www.notion.so/my-integrations> → **New integration**.
2. Name it something like "Partner Reporting Sync", workspace = the workspace
   that owns the databases.
3. Copy the "Internal Integration Token" — this is your `NOTION_API_KEY`.
4. Add it as a secret in:
   - GitHub → repo → Settings → Secrets and variables → Actions → `NOTION_API_KEY`
   - Vercel → Project → Env Vars → `NOTION_API_KEY` (if you want `/settings/notion` to surface live status from Vercel; not strictly required)

## 2. Create Notion databases

For each mart you want to sync, create a fresh Notion database with one
**title** property (we use the partner name) and any number of other properties
whose names you control. Supported Notion property types:

| Mart column kind | Notion property type |
|------------------|----------------------|
| text / string    | `title` or `rich_text` |
| number / money   | `number`             |
| percent          | `number`             |
| date             | `date`               |
| bool             | `checkbox`           |

Set the number format (e.g. Dollar, Percent) in Notion itself; we always write
a raw float. Percents are stored as 0..1.

## 3. Share each database with the integration

Open the database → `…` → **Connections → Add connections** → select your
integration. Without this, the integration can't see the database.

## 4. Configure a sync target in the web app

1. Go to `/settings/notion` in the deployed app.
2. Click "Add a new target".
3. Pick the source mart (e.g. `partner_rankings`).
4. Paste the Notion database ID (the 32-char hex from the Notion URL).
5. Check off the columns to push and rename the Notion property names as needed.
6. Apply optional filters (e.g. `period_type = all_time` on `partner_summary`).
7. Save. The next scheduled run (every 30 min) will sync it.

## 5. Manual sync

```bash
DATABASE_URL=... NOTION_API_KEY=... python scripts/notion_sync.py
```

Exits non-zero if any target fails; successful targets still run. The web UI
shows the most recent status + message per target on `/settings/notion`.

## What's NOT synced

Anything you don't explicitly add as a sync target. The dashboard, drilldowns,
forecasts, funnel analysis, and rep performance all live in the web app.
Notion is intentionally a subset — the "management brief", not the OLAP layer.
