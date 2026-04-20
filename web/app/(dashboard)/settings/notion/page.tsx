import { PageHeader, Panel } from "@/components/ui";
import { NOTION_SOURCES, listTargets } from "@/lib/notion-targets";
import { TargetForm } from "./TargetForm";
import { TargetRow } from "./TargetRow";

export const dynamic = "force-dynamic";

export default async function NotionSettingsPage() {
  const targets = await listTargets();
  return (
    <>
      <PageHeader
        title="Notion sync"
        subtitle="Pick which mart tables (and which columns) to push into Notion databases. Everything else stays in this app."
      />

      <Panel title="How this works" className="mb-6">
        <ol className="text-sm text-muted list-decimal ml-5 space-y-1">
          <li>In Notion, create a database for each mart you want to sync; add a Notion integration and share each database with it.</li>
          <li>Copy the integration token into <code>NOTION_API_KEY</code> (Vercel env var + GitHub Actions secret).</li>
          <li>Copy each database&apos;s ID into a new target below.</li>
          <li>The scheduled job (<code>scripts/notion_sync.py</code>) reads these targets and upserts rows every 30 minutes.</li>
        </ol>
      </Panel>

      <Panel title="Configured targets" className="mb-6">
        {targets.length === 0 ? (
          <div className="text-sm text-muted">No targets yet — add one below.</div>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-muted text-xs uppercase">
              <tr className="border-b border-border">
                <th className="text-left py-2 pr-3">Source</th>
                <th className="text-left py-2 pr-3">Notion DB</th>
                <th className="text-left py-2 pr-3">Enabled</th>
                <th className="text-left py-2 pr-3">Last sync</th>
                <th className="text-right py-2 pr-3"></th>
              </tr>
            </thead>
            <tbody>
              {targets.map((t) => (
                <TargetRow key={t.id} target={t} />
              ))}
            </tbody>
          </table>
        )}
      </Panel>

      <Panel title="Add a new target">
        <TargetForm sources={NOTION_SOURCES} />
      </Panel>
    </>
  );
}
