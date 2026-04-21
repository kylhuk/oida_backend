FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    gcc \
    git \
    libpq-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY infra ./infra

RUN pip install --upgrade pip setuptools wheel && pip install .

FROM base AS api
CMD ["uvicorn", "data_platform.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS worker
CMD ["celery", "-A", "data_platform.workers.celery_app:celery_app", "worker", "--loglevel=INFO"]

FROM base AS dagster-system
RUN mkdir -p ${DAGSTER_HOME} && \
    cp /app/infra/dagster/dagster.yaml ${DAGSTER_HOME}/dagster.yaml && \
    cp /app/infra/dagster/workspace.yaml ${DAGSTER_HOME}/workspace.yaml
WORKDIR ${DAGSTER_HOME}
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/dagster_home/workspace.yaml"]

FROM base AS dagster-code
EXPOSE 4000
HEALTHCHECK --interval=10s --timeout=3s --retries=10 CMD ["dagster", "api", "grpc-health-check", "-p", "4000"]
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-m", "data_platform.dagster_project.definitions"]
