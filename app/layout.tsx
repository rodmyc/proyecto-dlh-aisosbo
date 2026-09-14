import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AISOS Data Hub",
  description: "Centro de carga y trazabilidad del lakehouse de AISOS.",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="es" className="h-full antialiased">
      <body className="min-h-full">{children}</body>
    </html>
  );
}
