import { auth } from "@/lib/auth";

export default auth((req) => {
  const { pathname } = req.nextUrl;
  const isAuthed = !!req.auth;
  const isAuthRoute = pathname.startsWith("/login") || pathname.startsWith("/signup");

  if (!isAuthed && !isAuthRoute) {
    const url = req.nextUrl.clone();
    url.pathname = "/login";
    url.searchParams.set("from", pathname);
    return Response.redirect(url);
  }
  if (isAuthed && isAuthRoute) {
    const url = req.nextUrl.clone();
    url.pathname = "/";
    return Response.redirect(url);
  }
});

export const config = {
  matcher: ["/((?!api/auth|_next/static|_next/image|favicon.ico).*)"],
};
