FROM python:3.11-slim-bookworm AS builder

WORKDIR /build

COPY backend/deps.txt .
RUN pip install --no-cache-dir --user -r deps.txt

FROM python:3.11-slim-bookworm

RUN groupadd -r appuser && useradd -r -g appuser -u 1001 appuser

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /root/.local /home/appuser/.local

WORKDIR /app

COPY backend/app.py .
COPY web/ /web/

RUN chown -R appuser:appuser /app /web
RUN chmod -R 755 /web

USER appuser

ENV PATH=/home/appuser/.local/bin:$PATH
ENV DATA_PATH=/data

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -fsS http://localhost:8080/healthz || exit 1

LABEL org.opencontainers.image.title="Lance Viewer"
LABEL org.opencontainers.image.description="Read-only web viewer for Lance datasets"
LABEL org.opencontainers.image.source="https://github.com/your-username/lance-viewer"
LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.licenses="MIT"

CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]