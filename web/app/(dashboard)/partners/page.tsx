import Link from "next/link";
import { getPartnerRankings } from "@/lib/queries";
import { PageHeader, Panel, fmtInt, fmtMoney, fmtPct } from "@/components/ui";
import { QuadrantScatter } from "@/components/Chart";

export const revalidate = 300;

export default async function PartnersPage() {
  const rankings = await getPartnerRankings();
  const scatter = rankings.map((r) => ({
    partner_name: r.partner_name,
    total_leads: Number(r.total_leads),
    lead_to_won_rate: Number(r.lead_to_won_rate ?? 0),
    revenue_closed_won: Number(r.revenue_closed_won),
  }));

  return (
    <>
      <PageHeader title="Partner rankings" subtitle="All-time leaderboard with a volume-vs-efficiency quadrant." />
      <Panel title="Volume × efficiency (bubble size = revenue)" className="mb-6">
        <QuadrantScatter data={scatter} />
      </Panel>
      <Panel title={`${rankings.length} partners`}>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-muted text-xs uppercase">
              <tr className="border-b border-border">
                <th className="text-left py-2 pr-3">#</th>
                <th className="text-left py-2 pr-3">Partner</th>
                <th className="text-right py-2 pr-3">Leads</th>
                <th className="text-right py-2 pr-3">Deals won</th>
                <th className="text-right py-2 pr-3">Revenue</th>
                <th className="text-right py-2 pr-3">Lead → won</th>
                <th className="text-right py-2 pr-3">Rev / lead</th>
                <th className="text-right py-2 pr-3">Penetration</th>
                <th className="text-left py-2 pr-3">Quadrant</th>
              </tr>
            </thead>
            <tbody>
              {rankings.map((r) => (
                <tr key={r.partner_id} className="border-b border-border/50 hover:bg-bg">
                  <td className="py-2 pr-3">{r.rank_by_revenue}</td>
                  <td className="py-2 pr-3">
                    <Link href={`/partners/${r.partner_id}`} className="text-accent hover:underline">
                      {r.partner_name}
                    </Link>
                    {r.is_top_performer ? <span className="ml-2 text-xs text-good">★</span> : null}
                    {r.is_high_potential ? <span className="ml-2 text-xs text-warn">↑</span> : null}
                    {r.is_underperformer ? <span className="ml-2 text-xs text-bad">↓</span> : null}
                  </td>
                  <td className="py-2 pr-3 text-right">{fmtInt(r.total_leads)}</td>
                  <td className="py-2 pr-3 text-right">{fmtInt(r.deals_closed_won)}</td>
                  <td className="py-2 pr-3 text-right">{fmtMoney(Number(r.revenue_closed_won))}</td>
                  <td className="py-2 pr-3 text-right">{fmtPct(r.lead_to_won_rate, 2)}</td>
                  <td className="py-2 pr-3 text-right">{fmtMoney(Number(r.revenue_per_lead))}</td>
                  <td className="py-2 pr-3 text-right">{fmtPct((r.partner_penetration_rate || 0) / 100)}</td>
                  <td className="py-2 pr-3 text-xs text-muted">{r.volume_efficiency_quadrant}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </>
  );
}
