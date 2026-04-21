import NextAuth from "next-auth";
import Credentials from "next-auth/providers/credentials";
import bcrypt from "bcryptjs";
import { pool, APP_SCHEMA } from "./db";

export const { handlers, auth, signIn, signOut } = NextAuth({
  // next-auth v5 beta sometimes fails to auto-detect the secret in the edge
  // middleware runtime — pass it explicitly so we read both the canonical
  // AUTH_SECRET and the older NEXTAUTH_SECRET name.
  secret: process.env.AUTH_SECRET || process.env.NEXTAUTH_SECRET,
  // Vercel's proxy rewrites the host header; trusting it avoids
  // UntrustedHost errors on the production URL.
  trustHost: true,
  session: { strategy: "jwt" },
  pages: { signIn: "/login" },
  providers: [
    Credentials({
      credentials: {
        email: { label: "Email", type: "email" },
        password: { label: "Password", type: "password" },
      },
      async authorize(raw) {
        const email = String(raw?.email || "").trim().toLowerCase();
        const password = String(raw?.password || "");
        if (!email || !password) return null;

        const { rows } = await pool().query(
          `select id, email, name, password_hash, role
           from ${APP_SCHEMA}.users where email = $1 limit 1`,
          [email],
        );
        const user = rows[0];
        if (!user) return null;

        const ok = await bcrypt.compare(password, user.password_hash);
        if (!ok) return null;

        await pool().query(
          `update ${APP_SCHEMA}.users set last_login_at = now() where id = $1`,
          [user.id],
        );
        return {
          id: user.id,
          email: user.email,
          name: user.name,
          role: user.role,
        };
      },
    }),
  ],
  callbacks: {
    async jwt({ token, user }) {
      if (user) {
        token.role = (user as { role?: string }).role ?? "member";
      }
      return token;
    },
    async session({ session, token }) {
      if (session.user) {
        (session.user as { role?: string }).role =
          (token.role as string) ?? "member";
      }
      return session;
    },
  },
});
