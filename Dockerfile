# CPU-oriented image for CLI / API (GPU requires NVIDIA Container Toolkit + CUDA base — use a custom base if needed).
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src

RUN pip install --no-cache-dir uv \
    && uv sync --frozen --no-dev --extra api \
    && ln -sf /app/.venv/bin/yaruk /usr/local/bin/yaruk

ENV PATH="/app/.venv/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# Default: show CLI help (override in compose/k8s)
CMD ["yaruk", "--help"]
