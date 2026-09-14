import { createWriteStream } from "node:fs";
import { mkdir, rename, rm } from "node:fs/promises";
import { basename, extname, join, relative } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { randomUUID } from "node:crypto";
import { NextResponse } from "next/server";

import { launchIngestion } from "@/app/lib/dagster";
import { DATASETS, isDatasetKey } from "@/app/lib/datasets";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const INGESTION_ROOT = process.env.INGESTION_ROOT ?? "/opt/app/data/incoming";
const MAX_UPLOAD_BYTES = Number(process.env.MAX_UPLOAD_BYTES ?? 512 * 1024 * 1024);

function safeFilename(encodedName: string) {
  let decodedName: string;
  try {
    decodedName = decodeURIComponent(encodedName);
  } catch {
    throw new Error("El nombre del archivo no es válido.");
  }

  const cleanName = basename(decodedName)
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (!cleanName || cleanName.length > 180) {
    throw new Error("El nombre del archivo no es válido.");
  }
  return cleanName;
}

export async function POST(request: Request) {
  const dataset = request.headers.get("x-dataset") ?? "";
  const encodedName = request.headers.get("x-file-name") ?? "";
  const contentLength = Number(request.headers.get("content-length") ?? 0);

  if (!isDatasetKey(dataset)) {
    return NextResponse.json({ error: "Selecciona un tipo de información válido." }, { status: 400 });
  }
  if (!request.body || !Number.isFinite(contentLength) || contentLength <= 0) {
    return NextResponse.json({ error: "Selecciona un archivo con contenido." }, { status: 400 });
  }
  if (contentLength > MAX_UPLOAD_BYTES) {
    return NextResponse.json(
      { error: `El archivo supera el límite de ${Math.floor(MAX_UPLOAD_BYTES / 1024 / 1024)} MB.` },
      { status: 413 },
    );
  }

  let originalName: string;
  try {
    originalName = safeFilename(encodedName);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Nombre de archivo inválido." },
      { status: 400 },
    );
  }

  const expectedExtension = DATASETS[dataset].extension;
  if (extname(originalName).toLowerCase() !== expectedExtension) {
    return NextResponse.json(
      { error: `${DATASETS[dataset].label} requiere un archivo ${expectedExtension}.` },
      { status: 400 },
    );
  }

  const datasetDirectory = join(/* turbopackIgnore: true */ INGESTION_ROOT, dataset);
  const storedName = `${new Date().toISOString().slice(0, 10)}-${randomUUID()}-${originalName}`;
  const finalPath = join(datasetDirectory, storedName);
  const temporaryPath = `${finalPath}.uploading`;

  try {
    await mkdir(datasetDirectory, { recursive: true });
    const source = Readable.fromWeb(request.body as NodeReadableStream<Uint8Array>);
    await pipeline(source, createWriteStream(temporaryPath, { flags: "wx", mode: 0o644 }));
    await rename(temporaryPath, finalPath);

    const filePath = relative(INGESTION_ROOT, finalPath).replaceAll("\\", "/");
    const run = await launchIngestion(dataset, filePath);
    return NextResponse.json(
      {
        runId: run.runId,
        status: run.status,
        dataset,
        originalName,
        bytes: contentLength,
      },
      { status: 201 },
    );
  } catch (error) {
    await rm(temporaryPath, { force: true }).catch(() => undefined);
    console.error("Upload or Dagster launch failed", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? `No se pudo iniciar la carga: ${error.message}`
            : "No se pudo iniciar la carga.",
      },
      { status: 502 },
    );
  }
}
