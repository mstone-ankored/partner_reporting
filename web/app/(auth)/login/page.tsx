import { signIn } from "@/lib/auth";
import Link from "next/link";

export default function LoginPage({
  searchParams,
}: {
  searchParams: { from?: string; error?: string };
}) {
  async function action(formData: FormData) {
    "use server";
    const email = String(formData.get("email") || "");
    const password = String(formData.get("password") || "");
    const from = String(formData.get("from") || "/");
    await signIn("credentials", { email, password, redirectTo: from || "/" });
  }

  return (
    <main className="min-h-screen flex items-center justify-center">
      <form action={action} className="w-80 space-y-3 p-6 rounded bg-panel border border-border">
        <h1 className="text-lg font-semibold">Sign in</h1>
        {searchParams.error ? (
          <p className="text-bad text-sm">Sign-in failed. Check your email and password.</p>
        ) : null}
        <input type="hidden" name="from" value={searchParams.from || "/"} />
        <label className="block text-sm">
          Email
          <input
            name="email"
            type="email"
            required
            autoComplete="email"
            className="mt-1 w-full bg-bg border border-border rounded px-2 py-1.5"
          />
        </label>
        <label className="block text-sm">
          Password
          <input
            name="password"
            type="password"
            required
            autoComplete="current-password"
            className="mt-1 w-full bg-bg border border-border rounded px-2 py-1.5"
          />
        </label>
        <button
          type="submit"
          className="w-full bg-accent text-white rounded px-3 py-1.5 text-sm font-medium"
        >
          Sign in
        </button>
        <p className="text-xs text-muted">
          No account?{" "}
          <Link href="/signup" className="text-accent underline">
            Create one
          </Link>
        </p>
      </form>
    </main>
  );
}
