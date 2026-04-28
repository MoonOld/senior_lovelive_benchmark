import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Senior LoveLive Benchmark",
  description: "LoveLive event and song coverage dashboard",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN">
      <body>{children}</body>
    </html>
  );
}
