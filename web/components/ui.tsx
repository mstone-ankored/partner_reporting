import { clsx } from "clsx";

export function KpiCard({
  label,
  value,
  sub,
}: {
  label: string;
  value: string;
  sub?: string;
}) {
  return (
    <div className="rounded border border-border bg-panel p-4">
      <div className="text-xs uppercase tracking-wide text-muted">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value}</div>
      {sub ? <div className="mt-0.5 text-xs text-muted">{sub}</div> : null}
    </div>
  );
}

export function PageHeader({
  title,
  subtitle,
  right,
}: {
  title: string;
  subtitle?: string;
  right?: React.ReactNode;
}) {
  return (
    <div className="flex items-baseline justify-between mb-6">
      <div>
        <h1 className="text-xl font-semibold">{title}</h1>
        {subtitle ? <p className="text-sm text-muted mt-1">{subtitle}</p> : null}
      </div>
      {right}
    </div>
  );
}

export function Panel({
  title,
  children,
  className,
}: {
  title?: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section className={clsx("rounded border border-border bg-panel", className)}>
      {title ? (
        <header className="px-4 py-2 border-b border-border text-sm font-medium">
          {title}
        </header>
      ) : null}
      <div className="p-4">{children}</div>
    </section>
  );
}

export function fmtMoney(n: number | null | undefined): string {
  if (n == null || isNaN(n)) return "—";
  return n.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
}
export function fmtInt(n: number | null | undefined): string {
  if (n == null) return "—";
  return Number(n).toLocaleString();
}
export function fmtPct(n: number | null | undefined, digits = 1): string {
  if (n == null || isNaN(n)) return "—";
  return `${(n * 100).toFixed(digits)}%`;
}

// Format a date-or-ISO-string as short month + 4-digit year ("Jan 2025").
export function fmtMonthYear(d: string | Date | null | undefined): string {
  if (d == null) return "—";
  const date = typeof d === "string" ? new Date(d) : d;
  if (isNaN(date.getTime())) return "—";
  return date.toLocaleDateString("en-US", { month: "short", year: "numeric", timeZone: "UTC" });
}

// Short month + 2-digit year ("Jan 24"). Used for dense X-axis labels where
// space matters.
export function fmtMonthShortYear(d: string | Date | null | undefined): string {
  if (d == null) return "—";
  const date = typeof d === "string" ? new Date(d) : d;
  if (isNaN(date.getTime())) return "—";
  return date.toLocaleDateString("en-US", { month: "short", year: "2-digit", timeZone: "UTC" });
}

export function fmtDateShort(d: string | Date | null | undefined): string {
  if (d == null) return "—";
  const date = typeof d === "string" ? new Date(d) : d;
  if (isNaN(date.getTime())) return "—";
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "2-digit", timeZone: "UTC" });
}

export function fmtDaysBetween(
  from: string | Date | null | undefined,
  to: string | Date | null | undefined,
): string {
  if (from == null || to == null) return "—";
  const a = typeof from === "string" ? new Date(from) : from;
  const b = typeof to === "string" ? new Date(to) : to;
  if (isNaN(a.getTime()) || isNaN(b.getTime())) return "—";
  const days = (b.getTime() - a.getTime()) / 86_400_000;
  if (days < 0) return "—";
  if (days < 1) return `${(days * 24).toFixed(1)}h`;
  return `${days.toFixed(1)}d`;
}
