FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:$PATH"

# No log buffering
ENV PYTHONUNBUFFERED=1

COPY . ./service

WORKDIR /app/service

RUN uv pip install --system .

CMD ["sh", "-c", "gunicorn raw-data-service.main:app --workers ${GUNICORN_WORKERS:-8} --worker-class uvicorn.workers.UvicornWorker --backlog 8192 --bind 0.0.0.0:${PORT:-8001} --timeout ${GUNICORN_TIMEOUT:-120}"]
