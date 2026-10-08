import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "netlog-anomaly",
  description: "Window timeline and detector comparison for the BGL log dataset",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
