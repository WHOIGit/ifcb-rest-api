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

EXPOSE 8001

CMD ["uvicorn", "raw-data-service.main:app", "--host", "0.0.0.0", "--port", "8001"]
