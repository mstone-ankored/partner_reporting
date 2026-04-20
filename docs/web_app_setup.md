# Web app — local dev

The Next.js dashboard lives in [`../web`](../web). It reads mart tables from
Neon via `@neondatabase/serverless` and stores auth + Notion config in a
dedicated `partner_reporting_app` schema on the same Neon database.

## Local setup

```bash
cd web
cp .env.example .env.local    # then fill in DATABASE_URL + AUTH_SECRET + SIGNUP_SECRET
npm install
npm run migrate               # creates partner_reporting_app schema + tables
npm run dev                   # http://localhost:3000
```

## Env vars

| Name            | Required | Purpose                                               |
|-----------------|----------|-------------------------------------------------------|
| `DATABASE_URL`  | yes      | Neon pooled connection string                         |
| `AUTH_SECRET`   | yes      | NextAuth JWT signing key (`openssl rand -base64 32`)  |
| `AUTH_URL`      | prod     | Your deployed URL (e.g. `https://partner.yourco.com`) |
| `SIGNUP_SECRET` | yes      | Shared secret that gates new account creation         |
| `MART_SCHEMA`   | no       | Defaults to `partner_reporting`                       |
| `APP_SCHEMA`    | no       | Defaults to `partner_reporting_app`                   |

## Pages

- `/login`, `/signup` — NextAuth credentials (email + password, bcrypt).
- `/` — all-partner overview KPIs + top-10 revenue + monthly trend.
- `/partners` — leaderboard, volume × efficiency quadrant scatter.
- `/partners/[id]` — per-partner drilldown: KPIs, monthly trend, funnel, reps.
- `/trends` — monthly leads / deals won / revenue lines.
- `/forecast?partner=<id>` — interactive forecast with adjustable assumptions.
- `/settings/notion` — pick which mart tables push to which Notion databases.

## Build / deploy

Vercel autodetects Next.js — just set the **Root Directory** for the project to
`web/`. See `vercel_neon_setup.md` for the full walkthrough.
