FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

RUN groupadd --system dagster \
    && useradd --system --gid dagster --create-home dagster \
    && mkdir -p /opt/dagster/dagster_home /opt/dagster/storage /opt/dagster/data/staging \
    && chown -R dagster:dagster /opt/dagster

COPY pipeline/pyproject.toml /tmp/lakehouse-pipeline/pyproject.toml
COPY pipeline/src /tmp/lakehouse-pipeline/src

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir /tmp/lakehouse-pipeline \
    && rm -rf /tmp/lakehouse-pipeline

COPY --chown=dagster:dagster infra/dagster/dagster.yaml infra/dagster/workspace.yaml ${DAGSTER_HOME}/

USER dagster
WORKDIR /opt/dagster
