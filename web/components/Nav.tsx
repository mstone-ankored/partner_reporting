import Link from "next/link";
import { signOut } from "@/lib/auth";

const links = [
  { href: "/", label: "Overview" },
  { href: "/partners", label: "Partners" },
  { href: "/trends", label: "Trends" },
  { href: "/forecast", label: "Forecast" },
  { href: "/settings/partners", label: "Partner info" },
  { href: "/settings/notion", label: "Notion sync" },
];

export function Nav({ user }: { user?: { email?: string | null; name?: string | null } }) {
  return (
    <aside className="w-56 border-r border-border bg-panel min-h-screen flex flex-col">
      <div className="p-4 border-b border-border">
        <div className="text-sm font-semibold">Partner Reporting</div>
        <div className="text-xs text-muted">{user?.email}</div>
      </div>
      <nav className="flex-1 p-2 space-y-0.5">
        {links.map((l) => (
          <Link
            key={l.href}
            href={l.href}
            className="block px-3 py-1.5 rounded text-sm hover:bg-bg text-text"
          >
            {l.label}
          </Link>
        ))}
      </nav>
      <form
        action={async () => {
          "use server";
          await signOut({ redirectTo: "/login" });
        }}
        className="p-3 border-t border-border"
      >
        <button type="submit" className="text-xs text-muted hover:text-text">
          Sign out
        </button>
      </form>
    </aside>
  );
}
