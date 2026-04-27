import Link from "next/link";
import {
  getMonthlySummary,
  getPartnerFormSubmissions,
  getPartnerRankings,
  getTotals,
  PeriodFilter,
  PERIOD_LABELS,
  type PartnerFormSubmissionRow,
} from "@/lib/queries";
import {
  KpiCard,
  Panel,
  PageHeader,
  fmtDateShort,
  fmtDaysBetween,
  fmtInt,
  fmtMonthShortYear,
  fmtMoney,
  fmtPct,
} from "@/components/ui";
import { TrendLine, HBar } from "@/components/Chart";

export const dynamic = "force-dynamic";

const PERIODS: PeriodFilter[] = ["all", "ytd", "last_12m", "qtd", "mtd"];

function normalizePeriod(raw: string | string[] | undefined): PeriodFilter {
  const v = Array.isArray(raw) ? raw[0] : raw;
  return (PERIODS as string[]).includes(v ?? "") ? (v as PeriodFilter) : "all";
}

export default async function OverviewPage({
  searchParams,
}: {
  searchParams: { period?: string };
}) {
  const period = normalizePeriod(searchParams.period);
  const [totals, monthly, rankings, formSubmissionsRaw] = await Promise.all([
    getTotals(period),
    getMonthlySummary(),
    getPartnerRankings(),
    getPartnerFormSubmissions().catch((e) => {
      console.error("[overview] getPartnerFormSubmissions failed", e);
      return [] as PartnerFormSubmissionRow[];
    }),
  ]);
  const formSubmissions = formSubmissionsRaw;

  const topByRevenue = rankings
    .slice()
    .sort((a, b) => (b.revenue_closed_won || 0) - (a.revenue_closed_won || 0))
    .slice(0, 10)
    .map((r) => ({ partner_name: r.partner_name, revenue_closed_won: Number(r.revenue_closed_won) }));

  const mqlRate = totals.total_leads ? totals.total_mql / totals.total_leads : null;
  const leadToWon = totals.total_leads ? totals.deals_won / totals.total_leads : null;

  return (
    <>
      <PageHeader
        title="Partner overview"
        subtitle={`${PERIOD_LABELS[period]} — partner-sourced performance.`}
        right={
          <nav className="flex gap-1 text-xs">
            {PERIODS.map((p) => (
              <Link
                key={p}
                href={p === "all" ? "/" : `/?period=${p}`}
                className={
                  (p === period
                    ? "bg-accent text-white "
                    : "bg-panel text-muted hover:text-white ") +
                  "rounded border border-border px-2 py-1"
                }
              >
                {PERIOD_LABELS[p]}
              </Link>
            ))}
          </nav>
        }
      />
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-6">
        <KpiCard label="Active partners" value={fmtInt(totals.partners_active)} />
        <KpiCard label="Partner-sourced leads" value={fmtInt(totals.total_leads)} />
        <KpiCard label="MQL rate" value={fmtPct(mqlRate)} sub={`${fmtInt(totals.total_mql)} MQLs`} />
        <KpiCard
          label="Partner-sourced deals"
          value={fmtInt(totals.deals_total)}
          sub={`${fmtInt(totals.deals_won)} won`}
        />
        <KpiCard label="Lead → won" value={fmtPct(leadToWon, 2)} />
        <KpiCard label="Revenue" value={fmtMoney(totals.revenue)} />
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
        <Panel title="Monthly partner revenue">
          <TrendLine
            data={monthly.map((m) => ({
              month: fmtMonthShortYear(m.period_start_date),
              revenue: Number(m.revenue_closed_won),
              leads: m.total_leads,
            }))}
            xKey="month"
            yFormat="currency"
            yTickInterval={150_000}
            series={[
              { key: "revenue", label: "Revenue", color: "#4f8cff" },
              { key: "leads", label: "Leads", color: "#21c07a" },
            ]}
          />
        </Panel>
        <Panel title="Top 10 partners by revenue">
          <HBar data={topByRevenue} xKey="revenue_closed_won" yKey="partner_name" xFormat="currency" />
        </Panel>
      </div>

      <FormSubmissionsPanel rows={formSubmissions} />
    </>
  );
}

function FormSubmissionsPanel({ rows }: { rows: PartnerFormSubmissionRow[] }) {
  return (
    <Panel title={`Partner form submissions (${rows.length})`}>
      {rows.length === 0 ? (
        <div className="text-sm text-muted">No partner-referral form submissions yet.</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-muted text-xs uppercase">
              <tr className="border-b border-border">
                <th className="text-left py-2 pr-3">Submitted</th>
                <th className="text-left py-2 pr-3">Lead</th>
                <th className="text-left py-2 pr-3">Partner</th>
                <th className="text-left py-2 pr-3">Call held</th>
                <th className="text-left py-2 pr-3">Deal opened</th>
                <th className="text-right py-2 pr-3">Projected $</th>
                <th className="text-left py-2 pr-3">Stage</th>
                <th className="text-right py-2 pr-3">Form → Call</th>
                <th className="text-right py-2 pr-3">Call → Deal</th>
                <th className="text-right py-2 pr-3">Form → Deal</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const name = [r.contact_first_name, r.contact_last_name]
                  .filter(Boolean)
                  .join(" ");
                const leadLabel =
                  r.company_name ||
                  (name ? name : null) ||
                  r.contact_email ||
                  "—";
                return (
                  <tr key={r.submission_id} className="border-b border-border/50 hover:bg-bg">
                    <td className="py-2 pr-3 whitespace-nowrap">
                      {fmtDateShort(r.submitted_at)}
                    </td>
                    <td className="py-2 pr-3">
                      <div>{leadLabel}</div>
                      {r.contact_email && leadLabel !== r.contact_email ? (
                        <div className="text-xs text-muted">{r.contact_email}</div>
                      ) : null}
                    </td>
                    <td className="py-2 pr-3">{r.partner_name || "—"}</td>
                    <td className="py-2 pr-3 whitespace-nowrap">
                      {fmtDateShort(r.first_call_at)}
                    </td>
                    <td className="py-2 pr-3 whitespace-nowrap">
                      {fmtDateShort(r.deal_created_at)}
                    </td>
                    <td className="py-2 pr-3 text-right">
                      {r.amount != null ? fmtMoney(Number(r.amount)) : "—"}
                    </td>
                    <td className="py-2 pr-3 text-xs">
                      <span
                        className={
                          r.deal_status === "won"
                            ? "text-good"
                            : r.deal_status === "lost"
                              ? "text-bad"
                              : "text-muted"
                        }
                      >
                        {r.deal_stage || (r.deal_id ? r.deal_status : "no deal") || "—"}
                      </span>
                    </td>
                    <td className="py-2 pr-3 text-right">
                      {fmtDaysBetween(r.submitted_at, r.first_call_at)}
                    </td>
                    <td className="py-2 pr-3 text-right">
                      {fmtDaysBetween(r.first_call_at, r.deal_created_at)}
                    </td>
                    <td className="py-2 pr-3 text-right">
                      {fmtDaysBetween(r.submitted_at, r.deal_created_at)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}
