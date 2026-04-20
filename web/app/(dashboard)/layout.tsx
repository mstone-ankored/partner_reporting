import { auth } from "@/lib/auth";
import { Nav } from "@/components/Nav";

export default async function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const session = await auth();
  return (
    <div className="flex min-h-screen">
      <Nav user={session?.user || undefined} />
      <main className="flex-1 p-6">{children}</main>
    </div>
  );
}
