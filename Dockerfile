# ==========================================
# Stage 1: Build stage
# ==========================================
FROM python:3.12-slim-bookworm AS builder

# Set build-time environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Install system build dependencies (e.g. build-essential, curl for package building)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment to isolate dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade fundamental packaging tools
RUN pip install --upgrade pip setuptools wheel

# Copy configuration and setup files
COPY pyproject.toml README.md ./
# Copy source files to allow setuptools to package the project
COPY src/ ./src/

# Install the package and its dependencies (including optional kite package)
RUN pip install .[kite]

# ==========================================
# Stage 2: Production runtime stage
# ==========================================
FROM python:3.12-slim-bookworm AS runner

# Set production environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# Install runtime-only utilities (ca-certificates, curl, and gzip for reference gzipped files)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gzip \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Copy core repository directories needed at runtime (excluding tests to optimize image size)
COPY pyproject.toml README.md ./
COPY docs/ ./docs/
COPY infra/ ./infra/
COPY src/ ./src/

# Create necessary directories for local workspace/auditing and set permissions
RUN mkdir -p var/audit tmp/ input_drop && \
    chmod -R 777 var tmp input_drop

# Create a non-root system user and group for security hardening
RUN groupadd -g 10001 appgroup && \
    useradd -r -u 10001 -g appgroup -d /app -s /sbin/nologin appuser && \
    chown -R appuser:appgroup /app

# Switch to the non-root user
USER appuser

# Expose port 8080 (standard for Cloud Run services)
EXPOSE 8080

# Fail-closed default command: the Cloud Run Job entrypoint requires an
# explicit --spec-file argument and delegates only to the pinned-GCS daily
# pipeline CLI; it never falls back to the demo script. deploy.sh's
# --command/--args override this explicitly for the eod-swing job, but the
# image's own default must not be able to invoke the demo either.
CMD ["python", "-m", "india_swing.cloud_job"]
