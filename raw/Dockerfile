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

WORKDIR /app/service

COPY ./pyproject.toml .

RUN uv pip compile pyproject.toml -o requirements.txt
RUN uv pip install --system -r requirements.txt

COPY . /app/service

RUN uv pip install --system .

CMD ["sh", "-c", "uvicorn raw-data-service.main:app --host 0.0.0.0 --port ${PORT:-8001}"]
