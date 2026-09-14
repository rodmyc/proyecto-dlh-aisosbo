import { NextResponse } from "next/server";

import { getRun } from "@/app/lib/dagster";

export const dynamic = "force-dynamic";

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ runId: string }> },
) {
  const { runId } = await params;
  if (!/^[a-f0-9-]{32,36}$/i.test(runId)) {
    return NextResponse.json({ error: "Identificador de ejecución inválido." }, { status: 400 });
  }

  try {
    return NextResponse.json(await getRun(runId));
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "No se pudo consultar Dagster." },
      { status: 502 },
    );
  }
}
