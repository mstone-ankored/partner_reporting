import { getAllTimeTotals, getMonthlySummary, getPartnerRankings } from "@/lib/queries";
import { KpiCard, Panel, PageHeader, fmtInt, fmtMoney, fmtPct } from "@/components/ui";
import { TrendLine, HBar } from "@/components/Chart";

export const revalidate = 300;

export default async function OverviewPage() {
  const [totals, monthly, rankings] = await Promise.all([
    getAllTimeTotals(),
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
      <PageHeader title="Partner overview" subtitle="All-time performance across all partners." />
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-6">
        <KpiCard label="Active partners" value={fmtInt(totals.partners_active)} />
        <KpiCard label="Partner-sourced leads" value={fmtInt(totals.total_leads)} />
        <KpiCard label="MQL rate" value={fmtPct(mqlRate)} sub={`${fmtInt(totals.total_mql)} MQLs`} />
        <KpiCard label="Deals won" value={fmtInt(totals.deals_won)} />
        <KpiCard label="Lead → won" value={fmtPct(leadToWon, 2)} />
        <KpiCard label="Revenue" value={fmtMoney(totals.revenue)} />
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Panel title="Monthly partner revenue">
          <TrendLine
            data={monthly.map((m) => ({
              month: String(m.period_start_date).slice(0, 7),
              revenue: Number(m.revenue_closed_won),
              leads: m.total_leads,
            }))}
            xKey="month"
            series={[
              { key: "revenue", label: "Revenue ($)", color: "#4f8cff" },
              { key: "leads", label: "Leads", color: "#21c07a" },
            ]}
          />
        </Panel>
        <Panel title="Top 10 partners by revenue">
          <HBar data={topByRevenue} xKey="revenue_closed_won" yKey="partner_name" />
        </Panel>
      </div>
    </>
  );
}
