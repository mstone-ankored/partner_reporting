import { getMonthlySummary } from "@/lib/queries";
import { PageHeader, Panel } from "@/components/ui";
import { TrendLine } from "@/components/Chart";

export const revalidate = 300;

export default async function TrendsPage() {
  const monthly = await getMonthlySummary();
  const data = monthly.map((m) => ({
    month: String(m.period_start_date).slice(0, 7),
    leads: m.total_leads,
    deals_won: m.deals_closed_won,
    revenue: Number(m.revenue_closed_won),
  }));
  return (
    <>
      <PageHeader title="Trends" subtitle="All partners, monthly time series." />
      <div className="grid grid-cols-1 gap-4">
        <Panel title="Leads vs. deals won">
          <TrendLine
            data={data}
            xKey="month"
            series={[
              { key: "leads", label: "Leads", color: "#21c07a" },
              { key: "deals_won", label: "Deals won", color: "#f5a524" },
            ]}
            height={300}
          />
        </Panel>
        <Panel title="Revenue">
          <TrendLine
            data={data}
            xKey="month"
            series={[{ key: "revenue", label: "Revenue ($)", color: "#4f8cff" }]}
            height={300}
          />
        </Panel>
      </div>
    </>
  );
}
