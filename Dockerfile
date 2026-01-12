# ==========================================
# Stage 1: Builder
# ==========================================
FROM python:3.10-slim AS builder

ARG GITHUB_TOKEN

WORKDIR /build

# Install build dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       git \
       build-essential \
       libpq-dev \
       libmemcached-dev \
       zlib1g-dev \
       libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# CRITICAL: Create .netrc to authenticate HTTPS URLs automatically
RUN echo "machine github.com login ${GITHUB_TOKEN} password x-oauth-basic" > ~/.netrc \
    && chmod 600 ~/.netrc \
    && pip install --user --no-cache-dir -r requirements.txt \
    && rm ~/.netrc

# ==========================================
# Stage 2: Runtime
# ==========================================
FROM python:3.10-slim AS runtime

LABEL maintainer="Engineering Team <engineering@aviso.com>" \
      version="1.0.0" \
      description="Aviso Core Django Service"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AVISO_APPS=aviso_core \
    PATH="/home/appuser/.local/bin:$PATH"

WORKDIR /app

# Install runtime libs
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libpq5 \
       libmemcached11 \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' appuser

COPY --from=builder /root/.local /home/appuser/.local

COPY . /app

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

# Ensure Gunicorn is installed in requirements.txt!
CMD ["gunicorn", "aviso_core.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3"]