import Link from "next/link";
import { notFound } from "next/navigation";
import {
  getFunnelForPartner,
  getMonthlySummary,
  getPartnerAllTime,
  getRepPerfForPartner,
} from "@/lib/queries";
import { KpiCard, PageHeader, Panel, fmtInt, fmtMoney, fmtPct } from "@/components/ui";
import { TrendLine } from "@/components/Chart";

export const revalidate = 300;

export default async function PartnerDetailPage({
  params,
}: {
  params: { id: string };
}) {
  const [allTime, funnel, reps, monthly] = await Promise.all([
    getPartnerAllTime(params.id),
    getFunnelForPartner(params.id),
    getRepPerfForPartner(params.id),
    getMonthlySummary(params.id),
  ]);
  if (!allTime) notFound();

  return (
    <>
      <PageHeader
        title={allTime.partner_name}
        subtitle="All-time partner drilldown"
        right={
          <Link href={`/forecast?partner=${params.id}`} className="text-sm text-accent hover:underline">
            Build forecast →
          </Link>
        }
      />
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <KpiCard label="Leads" value={fmtInt(allTime.total_leads)} />
        <KpiCard label="MQL rate" value={fmtPct(allTime.mql_rate)} />
        <KpiCard label="Deals won" value={fmtInt(allTime.deals_closed_won)} />
        <KpiCard label="Revenue" value={fmtMoney(Number(allTime.revenue_closed_won))} />
        <KpiCard label="Lead → won" value={fmtPct(allTime.lead_to_won_rate, 2)} />
        <KpiCard label="Deal → won" value={fmtPct(allTime.deal_to_won_rate)} />
        <KpiCard label="Cycle (days)" value={allTime.avg_deal_cycle_days ? allTime.avg_deal_cycle_days.toFixed(0) : "—"} />
        <KpiCard label="Penetration" value={fmtPct((allTime.partner_penetration_rate || 0) / 100)} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
        <Panel title="Monthly">
          <TrendLine
            data={monthly.map((m) => ({
              month: String(m.period_start_date).slice(0, 7),
              leads: m.total_leads,
              deals_won: m.deals_closed_won,
              revenue: Number(m.revenue_closed_won),
            }))}
            xKey="month"
            series={[
              { key: "revenue", label: "Revenue ($)", color: "#4f8cff" },
              { key: "leads", label: "Leads", color: "#21c07a" },
              { key: "deals_won", label: "Deals won", color: "#f5a524" },
            ]}
          />
        </Panel>
        <Panel title="Funnel conversion">
          <table className="w-full text-sm">
            <thead className="text-muted text-xs uppercase">
              <tr className="border-b border-border">
                <th className="text-left py-1">Stage</th>
                <th className="text-right py-1">Entered</th>
                <th className="text-right py-1">Advanced</th>
                <th className="text-right py-1">Won</th>
                <th className="text-right py-1">Conv %</th>
                <th className="text-right py-1">Drop-off %</th>
              </tr>
            </thead>
            <tbody>
              {(funnel as Array<Record<string, unknown>>).map((r, i) => (
                <tr key={i} className="border-b border-border/50">
                  <td className="py-1">{String(r.deal_stage)}</td>
                  <td className="py-1 text-right">{fmtInt(Number(r.deals_entered))}</td>
                  <td className="py-1 text-right">{fmtInt(Number(r.deals_advanced))}</td>
                  <td className="py-1 text-right">{fmtInt(Number(r.deals_closed_won))}</td>
                  <td className="py-1 text-right">{fmtPct(Number(r.next_stage_conversion_rate))}</td>
                  <td className="py-1 text-right">{fmtPct(Number(r.drop_off_rate))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>
      </div>

      <Panel title="Reps on this partner's deals">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-muted text-xs uppercase">
              <tr className="border-b border-border">
                <th className="text-left py-2 pr-3">Rep</th>
                <th className="text-right py-2 pr-3">Deals</th>
                <th className="text-right py-2 pr-3">Won</th>
                <th className="text-right py-2 pr-3">Lost</th>
                <th className="text-right py-2 pr-3">Win %</th>
                <th className="text-right py-2 pr-3">Revenue</th>
                <th className="text-right py-2 pr-3">Avg deal</th>
                <th className="text-right py-2 pr-3">Cycle (d)</th>
              </tr>
            </thead>
            <tbody>
              {(reps as Array<Record<string, unknown>>).map((r, i) => (
                <tr key={i} className="border-b border-border/50">
                  <td className="py-2 pr-3">
                    {String(r.deal_owner_name || "—")}{" "}
                    <span className="text-xs text-muted">{String(r.deal_owner_email || "")}</span>
                  </td>
                  <td className="py-2 pr-3 text-right">{fmtInt(Number(r.deals_total))}</td>
                  <td className="py-2 pr-3 text-right">{fmtInt(Number(r.deals_won))}</td>
                  <td className="py-2 pr-3 text-right">{fmtInt(Number(r.deals_lost))}</td>
                  <td className="py-2 pr-3 text-right">{fmtPct(Number(r.win_rate))}</td>
                  <td className="py-2 pr-3 text-right">{fmtMoney(Number(r.revenue_closed_won))}</td>
                  <td className="py-2 pr-3 text-right">{fmtMoney(Number(r.avg_deal_size))}</td>
                  <td className="py-2 pr-3 text-right">
                    {r.avg_cycle_days ? Number(r.avg_cycle_days).toFixed(0) : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </>
  );
}
