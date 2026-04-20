import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Partner Reporting",
  description: "Partner performance analytics & forecasting",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
