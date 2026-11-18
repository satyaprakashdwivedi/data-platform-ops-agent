# Build from the repository root: docker build -f ops-tools/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app

COPY ops-tools/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock

COPY ops-tools/src /app/src
COPY db/schema.sql /app/db/schema.sql
COPY config /app/config
COPY pipelines /app/pipelines

RUN useradd --uid 10001 --create-home ops && mkdir -p /app/outbox && chown ops /app/outbox
USER ops
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=6 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=2)"
CMD ["uvicorn", "ops_tools.api:app", "--host", "0.0.0.0", "--port", "8000"]
