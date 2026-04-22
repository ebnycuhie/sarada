# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: runtime image ────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ffmpeg \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy Python packages from builder
COPY --from=builder /install /usr/local

# Non-root user for security
RUN groupadd --gid 1001 botuser \
    && useradd --uid 1001 --gid botuser --shell /bin/sh --create-home botuser

WORKDIR /app

# Copy application source
COPY src/ .

# Copy gallery-dl config — fixes Instagram 403 by setting proper headers,
# sleep intervals, and browser fingerprint.
# /etc/gallery-dl.conf is the system-wide config path gallery-dl reads
# automatically without any extra CLI flags.
COPY gallery-dl.conf /etc/gallery-dl.conf

# Pre-create data dirs
RUN mkdir -p /data/downloads /data/cookies /data/logs \
    && chown -R botuser:botuser /data /app

USER botuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DOWNLOAD_DIR=/data/downloads \
    COOKIES_DIR=/data/cookies \
    DATA_DIR=/data \
    LOG_DIR=/data/logs \
    MAX_SEND_FILES=20 \
    MAX_FILE_SIZE_MB=50 \
    MAX_CONCURRENT=1

HEALTHCHECK --interval=60s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import bot, config, downloader, storage, auth" || exit 1

CMD ["python", "-u", "bot.py"]
