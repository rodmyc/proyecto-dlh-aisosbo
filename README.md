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
- `compose.coolify.yaml`: despliegue productivo de Dagster OSS y PostgreSQL en Coolify.

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
web, el daemon, la ubicación de código y PostgreSQL. Solo `dagster-webserver`
debe recibir un dominio público en Coolify, apuntando a su puerto interno `3000`.
Los datos de PostgreSQL, los metadatos/logs de Dagster y el staging Parquet se
guardan en volúmenes persistentes independientes.
