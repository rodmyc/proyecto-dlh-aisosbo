import { createWriteStream } from "node:fs";
import { mkdir, rename, rm, stat } from "node:fs/promises";
import { basename, extname, join, relative } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import { NextResponse } from "next/server";

import { launchIngestion } from "@/app/lib/dagster";
import { DATASETS, isDatasetKey } from "@/app/lib/datasets";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const INGESTION_ROOT = process.env.INGESTION_ROOT ?? "/opt/app/data/incoming";
const MAX_UPLOAD_BYTES = Number(process.env.MAX_UPLOAD_BYTES ?? 512 * 1024 * 1024);
const MAX_CHUNK_BYTES = 10 * 1024 * 1024;
const UPLOAD_ID_PATTERN = /^[a-f0-9-]{36}$/i;

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

function integerHeader(request: Request, name: string) {
  const value = Number(request.headers.get(name));
  return Number.isSafeInteger(value) ? value : -1;
}

async function fileSize(path: string) {
  try {
    return (await stat(path)).size;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return 0;
    throw error;
  }
}

export async function POST(request: Request) {
  const dataset = request.headers.get("x-dataset") ?? "";
  const uploadId = request.headers.get("x-upload-id") ?? "";
  const encodedName = request.headers.get("x-file-name") ?? "";
  const chunkBytes = integerHeader(request, "content-length");
  const totalBytes = integerHeader(request, "x-file-size");
  const chunkOffset = integerHeader(request, "x-chunk-offset");

  if (!isDatasetKey(dataset)) {
    return NextResponse.json({ error: "Selecciona un tipo de información válido." }, { status: 400 });
  }
  if (!UPLOAD_ID_PATTERN.test(uploadId)) {
    return NextResponse.json({ error: "Identificador de subida inválido." }, { status: 400 });
  }
  if (!request.body || chunkBytes <= 0 || chunkBytes > MAX_CHUNK_BYTES) {
    return NextResponse.json({ error: "El bloque del archivo no es válido." }, { status: 400 });
  }
  if (totalBytes <= 0 || totalBytes > MAX_UPLOAD_BYTES) {
    return NextResponse.json(
      { error: `El archivo supera el límite de ${Math.floor(MAX_UPLOAD_BYTES / 1024 / 1024)} MB.` },
      { status: 413 },
    );
  }
  if (chunkOffset < 0 || chunkOffset + chunkBytes > totalBytes) {
    return NextResponse.json({ error: "La posición del bloque no es válida." }, { status: 400 });
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

  const uploadDirectory = join(/* turbopackIgnore: true */ INGESTION_ROOT, ".uploads");
  const partialPath = join(uploadDirectory, `${uploadId}.part`);

  try {
    await mkdir(uploadDirectory, { recursive: true });
    const receivedBefore = await fileSize(partialPath);
    if (receivedBefore !== chunkOffset) {
      return NextResponse.json(
        { error: "La subida debe reanudarse desde la última posición recibida.", receivedBytes: receivedBefore },
        { status: 409 },
      );
    }

    const source = Readable.fromWeb(request.body as NodeReadableStream<Uint8Array>);
    const flags = receivedBefore === 0 ? "wx" : "a";
    await pipeline(source, createWriteStream(partialPath, { flags, mode: 0o644 }));
    const receivedBytes = await fileSize(partialPath);
    if (receivedBytes !== chunkOffset + chunkBytes) {
      throw new Error("El tamaño recibido no coincide con el bloque enviado.");
    }
    if (receivedBytes < totalBytes) {
      return NextResponse.json({ uploadId, receivedBytes, complete: false }, { status: 202 });
    }

    const datasetDirectory = join(/* turbopackIgnore: true */ INGESTION_ROOT, dataset);
    await mkdir(datasetDirectory, { recursive: true });
    const storedName = `${new Date().toISOString().slice(0, 10)}-${uploadId}-${originalName}`;
    const finalPath = join(datasetDirectory, storedName);
    await rename(partialPath, finalPath);

    const filePath = relative(INGESTION_ROOT, finalPath).replaceAll("\\", "/");
    const run = await launchIngestion(dataset, filePath);
    return NextResponse.json(
      {
        runId: run.runId,
        status: run.status,
        dataset,
        originalName,
        bytes: totalBytes,
        receivedBytes,
        complete: true,
      },
      { status: 201 },
    );
  } catch (error) {
    console.error("Chunk upload or Dagster launch failed", error);
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

export async function DELETE(request: Request) {
  const uploadId = request.headers.get("x-upload-id") ?? "";
  if (!UPLOAD_ID_PATTERN.test(uploadId)) {
    return NextResponse.json({ error: "Identificador de subida inválido." }, { status: 400 });
  }

  const partialPath = join(
    /* turbopackIgnore: true */ INGESTION_ROOT,
    ".uploads",
    `${uploadId}.part`,
  );
  await rm(partialPath, { force: true });
  return new NextResponse(null, { status: 204 });
}
