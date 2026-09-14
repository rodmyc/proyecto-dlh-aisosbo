"use client";

import { ChangeEvent, DragEvent, useRef, useState } from "react";

import { DATASETS, DatasetKey } from "@/app/lib/datasets";

type Phase = "idle" | "uploading" | "launching" | "running" | "success" | "error";

type UploadResult = {
  runId: string;
  status: string;
  originalName: string;
  bytes: number;
};

type ChunkResponse = Partial<UploadResult> & {
  receivedBytes?: number;
  complete?: boolean;
  error?: string;
};

const MAX_UPLOAD_BYTES = 512 * 1024 * 1024;
const CHUNK_BYTES = 8 * 1024 * 1024;
const TERMINAL_STATUSES = new Set(["SUCCESS", "FAILURE", "CANCELED", "CANCELING"]);

function formatBytes(bytes: number) {
  return new Intl.NumberFormat("es-BO", { maximumFractionDigits: 1 }).format(bytes / 1024 / 1024);
}

function UploadIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 16V4m0 0L7.5 8.5M12 4l4.5 4.5M5 14v4a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-4" />
    </svg>
  );
}

function CheckIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="m5 12.5 4.2 4.2L19 7" />
    </svg>
  );
}

export default function UploadConsole() {
  const [dataset, setDataset] = useState<DatasetKey>("contacts");
  const [file, setFile] = useState<File | null>(null);
  const [phase, setPhase] = useState<Phase>("idle");
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState("");
  const [result, setResult] = useState<UploadResult | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const requestRef = useRef<XMLHttpRequest | null>(null);
  const uploadIdRef = useRef<string | null>(null);
  const cancelledRef = useRef(false);

  const datasetInfo = DATASETS[dataset];
  const isBusy = ["uploading", "launching", "running"].includes(phase);

  function chooseFile(candidate: File | null) {
    setError("");
    setResult(null);
    setPhase("idle");
    if (!candidate) {
      setFile(null);
      return;
    }
    if (!candidate.name.toLowerCase().endsWith(datasetInfo.extension)) {
      setFile(null);
      setError(`Este tipo de información requiere un archivo ${datasetInfo.extension}.`);
      return;
    }
    if (candidate.size > MAX_UPLOAD_BYTES) {
      setFile(null);
      setError("El archivo supera el límite de 512 MB.");
      return;
    }
    setFile(candidate);
  }

  function handleDatasetChange(event: ChangeEvent<HTMLSelectElement>) {
    const nextDataset = event.target.value as DatasetKey;
    setDataset(nextDataset);
    setFile(null);
    setError("");
    setResult(null);
    setPhase("idle");
    if (inputRef.current) inputRef.current.value = "";
  }

  function handleDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    setDragActive(false);
    if (!isBusy) chooseFile(event.dataTransfer.files.item(0));
  }

  async function watchRun(runId: string) {
    for (let attempt = 0; attempt < 900; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, 2_000));
      try {
        const response = await fetch(`/cargas/api/runs/${runId}`, { cache: "no-store" });
        const data = (await response.json()) as { status?: string; error?: string };
        if (!response.ok || !data.status) throw new Error(data.error || "No se pudo consultar la carga.");
        if (TERMINAL_STATUSES.has(data.status)) {
          if (data.status === "SUCCESS") {
            setPhase("success");
          } else {
            setPhase("error");
            setError(`Dagster terminó la ejecución con estado ${data.status}. Revisa el detalle técnico.`);
          }
          return;
        }
        setPhase("running");
      } catch (watchError) {
        setPhase("error");
        setError(watchError instanceof Error ? watchError.message : "No se pudo consultar la carga.");
        return;
      }
    }
    setPhase("error");
    setError("La carga continúa demasiado tiempo. Revisa su estado directamente en Dagster.");
  }

  function sendChunk(
    chunk: Blob,
    offset: number,
    uploadId: string,
    sourceFile: File,
  ): Promise<ChunkResponse> {
    return new Promise((resolve, reject) => {
      const request = new XMLHttpRequest();
      requestRef.current = request;
      request.open("POST", "/cargas/api/ingestions");
      request.setRequestHeader("content-type", "application/octet-stream");
      request.setRequestHeader("x-dataset", dataset);
      request.setRequestHeader("x-file-name", encodeURIComponent(sourceFile.name));
      request.setRequestHeader("x-upload-id", uploadId);
      request.setRequestHeader("x-file-size", String(sourceFile.size));
      request.setRequestHeader("x-chunk-offset", String(offset));

      request.upload.onprogress = (event) => {
        if (!event.lengthComputable) return;
        const uploaded = offset + event.loaded;
        setProgress(Math.round((uploaded / sourceFile.size) * 100));
        if (uploaded === sourceFile.size) setPhase("launching");
      };
      request.onerror = () => reject(new Error("Se interrumpió la conexión durante la subida."));
      request.onabort = () => reject(new DOMException("Subida cancelada", "AbortError"));
      request.onload = () => {
        requestRef.current = null;
        let payload: ChunkResponse;
        try {
          payload = JSON.parse(request.responseText) as ChunkResponse;
        } catch {
          reject(new Error(`El proxy devolvió una respuesta inválida (HTTP ${request.status}).`));
          return;
        }

        if ((request.status >= 200 && request.status < 300) || request.status === 409) {
          resolve(payload);
          return;
        }
        reject(new Error(payload.error || `La subida falló con HTTP ${request.status}.`));
      };
      request.send(chunk);
    });
  }

  async function startUpload() {
    if (!file || isBusy) return;
    setError("");
    setResult(null);
    setProgress(0);
    setPhase("uploading");
    cancelledRef.current = false;
    const uploadId = crypto.randomUUID();
    uploadIdRef.current = uploadId;

    try {
      let offset = 0;
      let failures = 0;
      while (offset < file.size) {
        const end = Math.min(offset + CHUNK_BYTES, file.size);
        let payload: ChunkResponse;
        try {
          payload = await sendChunk(file.slice(offset, end), offset, uploadId, file);
          failures = 0;
        } catch (uploadError) {
          if (cancelledRef.current || (uploadError instanceof DOMException && uploadError.name === "AbortError")) {
            return;
          }
          failures += 1;
          if (failures > 2) throw uploadError;
          await new Promise((resolve) => window.setTimeout(resolve, 800));
          continue;
        }

        if (typeof payload.receivedBytes !== "number") {
          throw new Error("El servidor no confirmó la posición recibida.");
        }
        offset = payload.receivedBytes;
        if (payload.complete) {
          if (!payload.runId || !payload.status || !payload.originalName || !payload.bytes) {
            throw new Error("Dagster no devolvió una ejecución válida.");
          }
          const completedUpload = payload as UploadResult;
          setResult(completedUpload);
          setPhase("running");
          uploadIdRef.current = null;
          void watchRun(completedUpload.runId);
          return;
        }
        setPhase("uploading");
      }
    } catch (uploadError) {
      setPhase("error");
      setError(
        uploadError instanceof Error
          ? `${uploadError.message} Puedes volver a intentarlo.`
          : "No se pudo completar la subida. Puedes volver a intentarlo.",
      );
    } finally {
      requestRef.current = null;
    }
  }

  function cancelUpload() {
    cancelledRef.current = true;
    requestRef.current?.abort();
    const uploadId = uploadIdRef.current;
    uploadIdRef.current = null;
    if (uploadId) {
      void fetch("/cargas/api/ingestions", {
        method: "DELETE",
        headers: { "x-upload-id": uploadId },
      });
    }
    setPhase("idle");
    setProgress(0);
  }

  function reset() {
    setFile(null);
    setResult(null);
    setError("");
    setProgress(0);
    setPhase("idle");
    if (inputRef.current) inputRef.current.value = "";
  }

  const statusCopy = {
    idle: file ? "Archivo listo para validar" : "Esperando archivo",
    uploading: `Subiendo archivo · ${progress}%`,
    launching: "Archivo recibido · iniciando Dagster",
    running: "Dagster está procesando la carga",
    success: "Carga completada",
    error: "La carga necesita atención",
  }[phase];

  return (
    <main className="upload-shell">
      <header className="topbar">
        <a className="brand" href="/cargas" aria-label="AISOS Data Hub, inicio">
          <span className="brand-mark">A</span>
          <span><strong>AISOS</strong><small>Data Hub</small></span>
        </a>
        <a className="dagster-link" href="/" target="_blank" rel="noreferrer">
          Abrir Dagster
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M8 5h11v11M19 5 5 19" /></svg>
        </a>
      </header>

      <section className="intro" aria-labelledby="page-title">
        <div>
          <p className="eyebrow">Centro de ingestión · Salesforce</p>
          <h1 id="page-title">Carga datos con trazabilidad completa.</h1>
          <p className="lede">Cada archivo se valida, conserva en Parquet y actualiza PostgreSQL sin duplicar registros.</p>
        </div>
        <div className="environment-badge"><span />Producción</div>
      </section>

      <section className="pipeline-strip" aria-label="Etapas del proceso">
        <div><span>01</span><strong>Archivo fuente</strong><small>CSV o Parquet</small></div>
        <i aria-hidden="true" />
        <div><span>02</span><strong>Staging</strong><small>Parquet Zstandard</small></div>
        <i aria-hidden="true" />
        <div><span>03</span><strong>Modelo analítico</strong><small>PostgreSQL</small></div>
      </section>

      <div className="workspace-grid">
        <section className="upload-panel" aria-labelledby="upload-title">
          <div className="panel-heading">
            <div><p className="section-label">Nueva carga</p><h2 id="upload-title">Selecciona el origen</h2></div>
            <span className={`status-chip status-${phase}`} aria-live="polite"><i />{statusCopy}</span>
          </div>

          <label className="field-label" htmlFor="dataset">Tipo de información</label>
          <div className="select-wrap">
            <select id="dataset" value={dataset} onChange={handleDatasetChange} disabled={isBusy}>
              {Object.entries(DATASETS).map(([key, value]) => <option key={key} value={key}>{value.label}</option>)}
            </select>
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m7 9.5 5 5 5-5" /></svg>
          </div>
          <p className="field-help">{datasetInfo.description}</p>

          <div
            className={`dropzone ${dragActive ? "is-dragging" : ""} ${file ? "has-file" : ""}`}
            onDragEnter={(event) => { event.preventDefault(); if (!isBusy) setDragActive(true); }}
            onDragOver={(event) => event.preventDefault()}
            onDragLeave={() => setDragActive(false)}
            onDrop={handleDrop}
          >
            <input
              ref={inputRef}
              id="source-file"
              type="file"
              accept={datasetInfo.extension}
              onChange={(event) => chooseFile(event.target.files?.item(0) ?? null)}
              disabled={isBusy}
            />
            <div className="drop-icon">{file ? <CheckIcon /> : <UploadIcon />}</div>
            {file ? (
              <><strong>{file.name}</strong><span>{formatBytes(file.size)} MB · {datasetInfo.extension.slice(1).toUpperCase()}</span></>
            ) : (
              <><strong>Arrastra el archivo aquí</strong><span>o selecciónalo desde tu equipo · máximo 512 MB</span></>
            )}
            <label htmlFor="source-file" className="secondary-button">{file ? "Cambiar archivo" : "Seleccionar archivo"}</label>
          </div>

          {phase === "uploading" && (
            <div className="progress-block" aria-live="polite">
              <div><span>Transfiriendo</span><strong>{progress}%</strong></div>
              <progress value={progress} max="100">{progress}%</progress>
            </div>
          )}

          {error && <div className="message error-message" role="alert"><strong>No se completó la carga</strong><span>{error}</span></div>}
          {phase === "success" && result && (
            <div className="message success-message" role="status"><strong>Datos actualizados</strong><span>Dagster terminó correctamente la ejecución.</span></div>
          )}

          <div className="form-actions">
            {phase === "uploading" ? (
              <button className="secondary-button" type="button" onClick={cancelUpload}>Cancelar subida</button>
            ) : phase === "success" ? (
              <button className="primary-button" type="button" onClick={reset}>Cargar otro archivo</button>
            ) : (
              <button className="primary-button" type="button" disabled={!file || isBusy} onClick={startUpload}>
                {phase === "launching" ? "Iniciando Dagster…" : phase === "running" ? "Procesando…" : "Validar e iniciar carga"}
              </button>
            )}
            {result?.runId && <a className="run-link" href={`/runs/${result.runId}`} target="_blank" rel="noreferrer">Ver ejecución {result.runId.slice(0, 8)}</a>}
          </div>
        </section>

        <aside className="rules-panel" aria-labelledby="rules-title">
          <p className="section-label">Antes de cargar</p>
          <h2 id="rules-title">Contrato del archivo</h2>
          <dl>
            <div><dt>Formato</dt><dd>{datasetInfo.extension.toUpperCase()}</dd></div>
            <div><dt>Llave única</dt><dd>{dataset === "historical_donations" ? "id_donacion" : "Id"}</dd></div>
            <div><dt>Estrategia</dt><dd>UPSERT incremental</dd></div>
          </dl>
          <div className="rule-note">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 8v4m0 4h.01M10.3 4.2 3.1 17a2 2 0 0 0 1.7 3h14.4a2 2 0 0 0 1.7-3L13.7 4.2a2 2 0 0 0-3.4 0Z" /></svg>
            <p><strong>No edites los encabezados.</strong> Usa el archivo exportado directamente desde Data Loader.</p>
          </div>
          <p className="privacy-note">Los archivos permanecen en la infraestructura privada y no se incluyen en GitHub.</p>
        </aside>
      </div>
    </main>
  );
}
