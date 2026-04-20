import { getMonthlySummary, getPartnerList } from "@/lib/queries";
import { PageHeader } from "@/components/ui";
import { ForecastClient } from "./ForecastClient";

export const revalidate = 300;

export default async function ForecastPage({
  searchParams,
}: {
  searchParams: { partner?: string };
}) {
  const partnerId = searchParams.partner;
  const [monthly, partners] = await Promise.all([
    getMonthlySummary(partnerId),
    getPartnerList(),
  ]);
  const history = monthly.map((m) => ({
    month: String(m.period_start_date).slice(0, 10),
    leads: m.total_leads,
    deals_won: m.deals_closed_won,
    revenue: Number(m.revenue_closed_won),
  }));

  return (
    <>
      <PageHeader
        title="Forecast"
        subtitle="Interactive projection built on the trailing monthly data. Adjust assumptions below."
      />
      <ForecastClient
        partners={partners}
        selectedPartner={partnerId ?? ""}
        history={history}
      />
    </>
  );
}
