# ---- build stage ----------------------------------------------------------
FROM python:3.12-slim AS build
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY collector ./collector

# Install into a virtualenv at a fixed path so it can be copied verbatim.
RUN python -m venv /venv \
 && /venv/bin/pip install --upgrade pip \
 && /venv/bin/pip install ".[dashboard]"

# ---- runtime stage --------------------------------------------------------
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/venv/bin:$PATH" \
    DATA_DIR=/data \
    SESSION_PATH=/data/collector.session \
    DATABASE_PATH=/data/collector.sqlite3 \
    LOG_DIR=/data/logs

RUN groupadd --system --gid 10001 collector \
 && useradd --system --uid 10001 --gid collector --home /data --shell /usr/sbin/nologin collector \
 && mkdir -p /data /app \
 && chown collector:collector /data

COPY --from=build /venv /venv
COPY --chmod=644 README.md /app/README.md

WORKDIR /app
USER collector
VOLUME ["/data"]

# The first login is interactive and must be run once:
#   docker compose run --rm collector python -m collector login
CMD ["python", "-m", "collector", "run"]
