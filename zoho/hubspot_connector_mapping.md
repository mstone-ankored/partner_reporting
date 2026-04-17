# HubSpot Connector Column Mapping

Zoho Analytics' HubSpot connector can name columns slightly differently
depending on your HubSpot edition, custom property setup, and the connector
version. Before pasting the Query Table SQL in this folder, open each raw
connector table in Zoho and verify the column names. If any differ, update
the SQL in the **stg_*** layer only — downstream tables reuse those renamed
aliases.

Below are the column names the SQL in `query_tables/` expects. The left
column is what I've coded against; the right is what you'll commonly see
from Zoho's connector. Both are usually identical; double-check custom
properties (partner fields especially).

## Contacts (→ `stg_contacts.sql`)

| Used in SQL                       | Common connector label                          |
|-----------------------------------|--------------------------------------------------|
| `Contact Id`                      | `Contact Id`, `Record Id`                        |
| `Email`                           | `Email`                                          |
| `First Name`, `Last Name`         | `First Name`, `Last Name`                        |
| `Company Name`                    | `Company Name`, `Company`                        |
| `Job Title`                       | `Job Title`                                      |
| `Industry`                        | `Industry`                                       |
| `Number Of Employees`             | `Number Of Employees`                            |
| `Annual Revenue`                  | `Annual Revenue`                                 |
| `Lifecycle Stage`                 | `Lifecycle Stage`                                |
| `Lead Status`                     | `Lead Status`, `HubSpot Lead Status`             |
| `Create Date`                     | `Create Date`                                    |
| `Became A Lead Date`              | `Became A Lead Date`                             |
| `Became A Marketing Qualified Lead Date` | same                                      |
| `Became A Sales Qualified Lead Date`     | same                                      |
| `Became An Opportunity Date`      | same                                             |
| `Became A Customer Date`          | same                                             |
| `Original Source`                 | `Original Source`                                |
| `Original Source Drill Down 1/2`  | same                                             |
| `Owner`                           | `HubSpot Owner`, `Contact Owner`                 |
| **`Referring Partner`** (custom)  | whatever your team named the partner property    |
| **`Partner Source Type`** (custom) | optional; used to explicitly mark form vs email |

## Deals (→ `stg_deals.sql`)

| Used in SQL                       | Common label                                    |
|-----------------------------------|-------------------------------------------------|
| `Deal Id`                         | `Deal Id`, `Record Id`                          |
| `Deal Name`                       | `Deal Name`                                     |
| `Pipeline`                        | `Pipeline`                                      |
| `Deal Stage`                      | `Deal Stage`                                    |
| `Amount`                          | `Amount`                                        |
| `Amount In Company Currency`      | same                                            |
| `Create Date`, `Close Date`       | same                                            |
| `Date Entered Closed Won`         | `Date Entered Closed Won`                       |
| `Is Deal Closed Won`              | `Is Deal Closed Won` (boolean-ish string)       |
| `Is Deal Closed`                  | `Is Deal Closed`                                |
| `Deal Type`                       | `Deal Type`                                     |
| `Deal Owner`                      | `Deal Owner`, `HubSpot Owner`                   |
| **`Referring Partner`** (custom)  | optional deal-level partner property            |

## Engagements (→ `stg_engagements.sql`)

| Used in SQL                       | Common label                                    |
|-----------------------------------|-------------------------------------------------|
| `Engagement Id`                   | `Engagement Id`                                 |
| `Engagement Type`                 | `Engagement Type`                               |
| `Engagement Timestamp`            | `Timestamp`, `Activity Date`                    |
| `Owner`                           | `Owner`, `HubSpot Owner`                        |
| `Email Direction`                 | `Email Direction` (for email engagements only)  |
| `Email From Address`              | `Email From Address`                            |
| `Email Subject`                   | `Email Subject`                                 |

## Engagement Contacts (→ `stg_engagement_contacts.sql`)

Usually a table named `Engagement Contacts` or `Engagement To Contact` with
just two columns (`Engagement Id`, `Contact Id`).

## Form Submissions (→ `stg_form_submissions.sql`)

Zoho lands form submissions as one row per (contact, form, submission). The
form-field columns vary by your HubSpot form schema:

| Used in SQL                   | Typical label in connector               |
|-------------------------------|-------------------------------------------|
| `Submission Id`               | `Submission Id`, `Conversion Id`          |
| `Contact Id`                  | `Contact Id`                              |
| `Form Id`                     | `Form Id`                                 |
| `Submission Timestamp`        | `Submission Timestamp`                    |
| `Page Url`                    | `Page Url`                                |
| **`Partner Name`** (custom)   | name of the partner field in your forms   |
| **`Referring Partner`** (custom) | alternative partner field              |
| **`Partner Referral`** (custom)  | alternative partner field              |
| `Company`, `Company Size`, `Industry`, `Country`, `Use Case` | standard form fields |

The **bolded** rows are where you most likely need to adjust — these are
whatever you named the partner-identifying field on your forms.

## Deal Contacts (→ `stg_deal_contacts.sql`)

The Deal↔Contact association table, typically with:
- `Deal Id`
- `Contact Id`
- `Is Primary` (boolean string)

## Owners (→ `stg_owners.sql`)

| Used in SQL        | Common label           |
|--------------------|------------------------|
| `Owner Id`         | `Owner Id`             |
| `Email`            | `Email`                |
| `First Name`       | `First Name`           |
| `Last Name`        | `Last Name`            |
| `Team Id`          | `Team Id`              |
| `Create Date`      | `Create Date`          |
| `Archived`         | `Archived`, `Is Archived` |

## Deal Stage History (→ `stg_deal_stage_history.sql`)

Optional — not all connector versions expose this. If you don't see a
`Deal Stage History` table in your Data Sources:

1. Go to **Data Source → HubSpot → Configure Sync**.
2. Under **Additional Objects**, check **Deal Property History** or
   **Deal Stage History** (label varies).
3. Re-sync.

If the connector still doesn't expose it, skip `07_stg_deal_stage_history.sql`,
`13_int_deal_stage_durations.sql`, and `22_partner_funnel_stage_conversion.sql` —
every other chart still works.

## Verifying

After you've built the `stg_*` layer, sanity-check each with:

```sql
SELECT COUNT(*) FROM "stg_contacts";           -- should match your HubSpot contacts
SELECT COUNT(*) FROM "stg_deals";              -- should match HubSpot deals
SELECT COUNT(*) FROM "stg_form_submissions";   -- should match submission count
```

Then for partner attribution:

```sql
SELECT attribution_method, COUNT(*)
FROM "int_partner_contact_attribution"
GROUP BY attribution_method;
```

Seeing zero rows here means no contacts matched any of the four attribution
methods. The most likely causes are (a) `ref_partners.csv` partner names /
domains don't match what's in HubSpot, or (b) the custom `Referring Partner`
property hasn't been named consistently.
