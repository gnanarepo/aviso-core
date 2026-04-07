# ==========================================
# Stage 1: Builder
# ==========================================
FROM python:3.10-slim AS builder

WORKDIR /build

# Install build dependencies (only what is needed to compile deps like lxml, cryptography etc.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libpq-dev \
       libmemcached-dev \
       zlib1g-dev \
       libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy wheels and requirements first (better layer caching)
COPY wheels/ /wheels/
COPY requirements.txt .

# Install internal wheels first, then remaining dependencies
RUN pip install --user --no-cache-dir /wheels/*.whl \
    && pip install --user --no-cache-dir -r requirements.txt


# ==========================================
# Stage 2: Runtime
# ==========================================
FROM python:3.10-slim AS runtime

LABEL maintainer="Engineering Team <engineering@aviso.com>" \
      version="1.0.0" \
      description="Aviso Core Django Service"

# Create non-root user
RUN adduser --disabled-password --gecos '' appuser

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AVISO_APPS=aviso_core \
    PATH="/home/appuser/.local/bin:$PATH"

WORKDIR /app

# Install runtime-only system libraries
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libpq5 \
       libmemcached11 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder --chown=appuser:appuser /root/.local /home/appuser/.local

# Copy application code
COPY --chown=appuser:appuser . /app

USER appuser

EXPOSE 8000

CMD ["gunicorn", "aviso_core.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3", "--timeout", "900"]