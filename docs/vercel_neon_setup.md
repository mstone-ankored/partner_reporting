# Vercel + Neon setup

End-to-end: provision the Neon Postgres database via Vercel's Neon integration,
wire it to both the dbt project and the Next.js app, then deploy.

## 1. Provision Neon via Vercel

1. In Vercel, create a new project (point it at this repo; the root directory
   for the web app is `web/`).
2. In the project dashboard, go to **Storage → Create Database → Neon**.
3. Vercel provisions a Neon database and auto-injects connection strings as
   environment variables on the Vercel project. The ones we use:
   - `DATABASE_URL` — pooled connection (what the app + notion_sync use).
   - `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE`
     — individual parts (useful for dbt).

## 2. Configure the Vercel project

Add these env vars (Project → Settings → Environment Variables, include Preview + Production):

| Name              | Source                                                  |
|-------------------|---------------------------------------------------------|
| `DATABASE_URL`    | auto-added by Neon integration (pooled)                 |
| `AUTH_SECRET`     | `openssl rand -base64 32`                               |
| `AUTH_URL`        | the deployed URL (e.g. `https://partner.yourco.com`)    |
| `SIGNUP_SECRET`   | shared secret for self-signup; rotate when people leave |
| `MART_SCHEMA`     | `partner_reporting` (default — can override)            |
| `APP_SCHEMA`      | `partner_reporting_app` (default)                       |
| `NOTION_API_KEY`  | optional; only if you want sync to run from Vercel      |

The **root directory** for the Vercel project is `web/`. Vercel's Next.js
build preset handles everything else.

## 3. Initialize the app schema

Run the migrations once (creates the `partner_reporting_app` schema, users,
sessions, notion sync config tables):

```bash
cd web
DATABASE_URL="postgres://…" npm run migrate
```

## 4. Point dbt at the same Neon database

Locally, copy `profiles.yml.example` to `~/.dbt/profiles.yml` and fill in:

```bash
export NEON_HOST=ep-xxx-pooler.us-east-1.aws.neon.tech
export NEON_USER=...
export NEON_PASSWORD=...
export NEON_DATABASE=neondb
make install
make build
```

For CI, set the same env vars as GitHub Actions secrets (see `refresh.yml`).

## 5. First sign-in

- Hit `/signup` on the deployed site.
- Enter the `SIGNUP_SECRET` you configured. The first account created is
  granted role `admin`; everyone after is `member`.

## 6. Optional: connect Notion

See [`notion_setup.md`](notion_setup.md).
