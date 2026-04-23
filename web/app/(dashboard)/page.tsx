import Link from "next/link";
import { getMonthlySummary, getPartnerRankings, getTotals, PeriodFilter, PERIOD_LABELS } from "@/lib/queries";
import { KpiCard, Panel, PageHeader, fmtInt, fmtMonthYear, fmtMoney, fmtPct } from "@/components/ui";
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
  const [totals, monthly, rankings] = await Promise.all([
    getTotals(period),
    getMonthlySummary(),
    getPartnerRankings(),
  ]);

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
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Panel title="Monthly partner revenue">
          <TrendLine
            data={monthly.map((m) => ({
              month: fmtMonthYear(m.period_start_date),
              revenue: Number(m.revenue_closed_won),
              leads: m.total_leads,
            }))}
            xKey="month"
            yFormat="currency"
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
    </>
  );
}
