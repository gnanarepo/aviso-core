# ==========================================
# Stage 1: Builder
# ==========================================
FROM python:3.10-slim AS builder

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

# SECURE FIX: Use a secret mount instead of ARG.
# This mounts the token only for this command, leaving no trace in the image history.
RUN --mount=type=secret,id=github_token \
    echo "machine github.com login $(cat /run/secrets/github_token) password x-oauth-basic" > ~/.netrc \
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

# Create the user first so we can use it for permissions
RUN adduser --disabled-password --gecos '' appuser

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AVISO_APPS=aviso_core \
    # Update PATH so python finds the installed scripts
    PATH="/home/appuser/.local/bin:$PATH"

WORKDIR /app

# Install runtime libs
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libpq5 \
       libmemcached11 \
    && rm -rf /var/lib/apt/lists/*

# FIX: Copy artifacts and change ownership immediately
# The files in builder are owned by root (/root/.local).
# We must chown them to appuser when copying to /home/appuser/.local
COPY --from=builder --chown=appuser:appuser /root/.local /home/appuser/.local

COPY --chown=appuser:appuser . /app

USER appuser

EXPOSE 8000

CMD ["gunicorn", "aviso_core.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3"]