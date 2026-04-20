"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";

export default function SignupPage() {
  const router = useRouter();
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setBusy(true);
    const data = new FormData(e.currentTarget);
    const body = {
      email: String(data.get("email") || ""),
      password: String(data.get("password") || ""),
      name: String(data.get("name") || ""),
      signupSecret: String(data.get("signupSecret") || ""),
    };
    const res = await fetch("/api/signup", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    setBusy(false);
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}));
      setError(payload.error || "Signup failed");
      return;
    }
    router.push("/login");
  }

  return (
    <main className="min-h-screen flex items-center justify-center">
      <form onSubmit={onSubmit} className="w-96 space-y-3 p-6 rounded bg-panel border border-border">
        <h1 className="text-lg font-semibold">Create account</h1>
        {error ? <p className="text-bad text-sm">{error}</p> : null}
        <label className="block text-sm">
          Name
          <input name="name" required className="mt-1 w-full bg-bg border border-border rounded px-2 py-1.5" />
        </label>
        <label className="block text-sm">
          Email
          <input name="email" type="email" required className="mt-1 w-full bg-bg border border-border rounded px-2 py-1.5" />
        </label>
        <label className="block text-sm">
          Password
          <input name="password" type="password" required minLength={8} className="mt-1 w-full bg-bg border border-border rounded px-2 py-1.5" />
        </label>
        <label className="block text-sm">
          Signup secret
          <input name="signupSecret" required className="mt-1 w-full bg-bg border border-border rounded px-2 py-1.5" />
          <span className="text-xs text-muted">Ask an admin for the current secret.</span>
        </label>
        <button disabled={busy} type="submit" className="w-full bg-accent text-white rounded px-3 py-1.5 text-sm font-medium disabled:opacity-60">
          {busy ? "Creating…" : "Create account"}
        </button>
        <p className="text-xs text-muted">
          Already have one?{" "}
          <Link href="/login" className="text-accent underline">
            Sign in
          </Link>
        </p>
      </form>
    </main>
  );
}
