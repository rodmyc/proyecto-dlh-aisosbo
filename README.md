# Pipeline ELT / Lakehouse

Monorepo inicial para extraer datos de Salesforce, almacenarlos en una capa de
staging Parquet, transformarlos con Polars y publicar modelos analíticos en
PostgreSQL. Dagster orquesta el pipeline y Next.js sirve como frontend.

## Componentes

- `app/`: frontend Next.js 16, React 19, TypeScript y Tailwind CSS.
- `pipeline/`: proyecto Python 3.13 con Dagster, Polars, Psycopg y el cliente de Salesforce.
- `data/staging/`: archivos Parquet generados localmente (ignorados por Git).
- `infra/postgres/init/`: inicialización de los esquemas PostgreSQL.
- `compose.yaml`: PostgreSQL local reproducible; la misma definición sirve como base para la VPS.
- `compose.coolify.yaml`: despliegue productivo de Dagster OSS conectado a un PostgreSQL externo en Coolify.

## Requisitos

- Node.js 20.9 o superior (se detectó Node.js 22).
- Python 3.13.
- Docker Engine con Docker Compose para ejecutar PostgreSQL.

## Configuración inicial

En PowerShell, desde la raíz del repositorio:

```powershell
Copy-Item .env.example .env
py -3.13 -m venv pipeline/.venv
pipeline/.venv/Scripts/python -m pip install --editable ./pipeline --group dev
```

Antes de iniciar PostgreSQL, cambia `POSTGRES_PASSWORD` en `.env`. Luego:

```powershell
docker compose up -d postgres
```

## Desarrollo

Frontend (puerto 3000):

```powershell
npm run dev
```

Dagster (por defecto también intenta usar el puerto 3000, por eso se asigna 3001):

```powershell
Set-Location pipeline
.venv/Scripts/dg dev --port 3001
```

La definición inicial `staging_toolchain_check` permite materializar un archivo
Parquet pequeño y comprobar que Dagster, Polars y la ruta de staging funcionan.

El job `ingest_export_job` procesa un archivo por ejecución. Requiere dos valores
de configuración: `dataset` y `file_path`. La ruta siempre es relativa a
`INGESTION_ROOT`; las rutas absolutas y los recorridos fuera de ese directorio se
rechazan. Los datasets admitidos son `campaigns`, `contacts`, `accounts`,
`recurring_donations`, `opportunities` y `historical_donations`.

Cada ejecución calcula SHA-256, evita reprocesar archivos idénticos, genera un
Parquet Zstandard en staging y usa `COPY` más `ON CONFLICT` para cargar PostgreSQL.
Los resultados por lote quedan en `etl.ingestion_batch`. Las oportunidades
mantienen su estado vigente en `raw.salesforce_opportunity_current` y su evolución
en `history.salesforce_opportunity_status`. El kardex completo se consulta en
`analytics.donor_kardex`; los pagos efectivos están en
`analytics.fact_effective_payment`.

## Verificación

```powershell
npm run lint
npm run build
pipeline/.venv/Scripts/python -m pytest pipeline/tests
pipeline/.venv/Scripts/ruff check pipeline
```

Las credenciales de Salesforce y PostgreSQL se leen desde variables de entorno.
El archivo `.env` nunca debe versionarse; `.env.example` solo documenta las claves.

## Despliegue en Coolify

El archivo `compose.coolify.yaml` construye servicios separados para el servidor
web, el daemon, la ubicación de código y la pasarela autenticada. El proceso de
código inicializa de forma idempotente los esquemas antes de arrancar. El stack
no ejecuta un servidor PostgreSQL.
La aplicación debe conectarse a la red predefinida de Coolify y recibe la conexión
mediante `DAGSTER_PG_HOST`, `DAGSTER_PG_PORT`, `DAGSTER_PG_DB`,
`DAGSTER_PG_USER` y `DAGSTER_PG_PASSWORD`. Solo `dagster-gateway` recibe el
dominio público, apuntando a su puerto interno `8080`.

Los datos analíticos y los metadatos de Dagster quedan en el PostgreSQL externo.
Los logs/metadatos locales de ejecución y el staging Parquet se conservan en los
volúmenes persistentes de la aplicación. Los archivos recibidos se guardan en el
volumen `incoming_data`, que posteriormente compartirá el frontend Next.js.
