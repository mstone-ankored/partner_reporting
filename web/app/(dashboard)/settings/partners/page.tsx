import { PageHeader, Panel } from "@/components/ui";
import { listPartnersWithCustomerCounts, listCustomerCountHistory } from "@/lib/partners-info";
import { PartnerRow } from "./PartnerRow";

export const dynamic = "force-dynamic";

export default async function PartnerInfoPage() {
  const partners = await listPartnersWithCustomerCounts();
  // Pull history for every partner in parallel. Volume is small (one row per
  // seed partner, maybe a handful of history rows each) so this is cheap.
  const histories = await Promise.all(
    partners.map((p) => listCustomerCountHistory(p.partner_id)),
  );

  return (
    <>
      <PageHeader
        title="Partner info"
        subtitle="Hand-entered partner metadata. Customer counts feed the penetration chart."
      />

      <Panel title="How this works" className="mb-6">
        <ul className="text-sm text-muted list-disc ml-5 space-y-1">
          <li>
            <strong>Name, domain, tier, start date</strong> come from{" "}
            <code>seeds/ref_partners.csv</code>. Edit there + re-run the refresh
            to change them.
          </li>
          <li>
            <strong>Customer count</strong> is the partner&apos;s total customer
            base (not ours). Used as the denominator in
            partner penetration = our customers ÷ their customers.
          </li>
          <li>
            Add a dated entry whenever you get a fresh number from the partner.
            History is preserved, so penetration-over-time on the trends page
            tracks your relationship depth correctly.
          </li>
        </ul>
      </Panel>

      <Panel title={`Partners (${partners.length})`}>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-muted text-xs uppercase">
              <tr className="border-b border-border">
                <th className="text-left py-2 pr-3">Partner</th>
                <th className="text-left py-2 pr-3">Domain</th>
                <th className="text-left py-2 pr-3">Tier</th>
                <th className="text-left py-2 pr-3">Started</th>
                <th className="text-right py-2 pr-3">Customer count</th>
                <th className="text-left py-2 pr-3">As of</th>
                <th className="text-right py-2 pr-3"></th>
              </tr>
            </thead>
            <tbody>
              {partners.map((p, i) => (
                <PartnerRow key={p.partner_id} partner={p} history={histories[i]} />
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </>
  );
}
