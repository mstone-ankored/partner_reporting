import { NextRequest, NextResponse } from "next/server";
import bcrypt from "bcryptjs";
import { z } from "zod";
import { pool, APP_SCHEMA } from "@/lib/db";

const Body = z.object({
  email: z.string().email(),
  password: z.string().min(8).max(128),
  name: z.string().min(1).max(120),
  signupSecret: z.string().min(1),
});

export async function POST(req: NextRequest) {
  const expected = process.env.SIGNUP_SECRET;
  if (!expected) {
    return NextResponse.json(
      { error: "Signup is disabled (no SIGNUP_SECRET set)" },
      { status: 503 },
    );
  }

  const parsed = Body.safeParse(await req.json().catch(() => null));
  if (!parsed.success) {
    return NextResponse.json({ error: "Invalid input" }, { status: 400 });
  }
  const { email, password, name, signupSecret } = parsed.data;
  if (signupSecret !== expected) {
    return NextResponse.json({ error: "Wrong signup secret" }, { status: 403 });
  }

  const normalized = email.trim().toLowerCase();
  const hash = await bcrypt.hash(password, 12);

  // First user to sign up becomes admin. After that, role defaults to member.
  const { rows: countRows } = await pool().query(
    `select count(*)::int as n from ${APP_SCHEMA}.users`,
  );
  const role = countRows[0]?.n === 0 ? "admin" : "member";

  try {
    await pool().query(
      `insert into ${APP_SCHEMA}.users (email, password_hash, name, role)
       values ($1, $2, $3, $4)`,
      [normalized, hash, name, role],
    );
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    if (msg.includes("duplicate key")) {
      return NextResponse.json(
        { error: "An account with that email already exists" },
        { status: 409 },
      );
    }
    throw e;
  }
  return NextResponse.json({ ok: true });
}
