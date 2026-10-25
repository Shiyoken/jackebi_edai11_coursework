
# STAGE 1: Builder
FROM python:3.10-slim AS builder 
WORKDIR /build 
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv 
ENV PATH="/opt/venv/bin:$PATH"

COPY docker/requirements.txt /build/requirements.txt 
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /build/requirements.txt

# STAGE 2: Runner
FROM python:3.10-slim AS runner 
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \ 
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv 
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Security: Create non-root system user and group
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /bin/bash -m appuser && \
    chown -R appuser:appgroup /app
# Switch to non-root user
USER appuser

# Copy application source code (last layer, frequently modified)
COPY --chown=appuser:appgroup data_generator/ /app/data_generator/

CMD ["python", "--version"]