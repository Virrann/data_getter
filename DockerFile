FROM python:3.14-slim

ENV POETRY_VERSION=2.2.1 \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install "poetry==$POETRY_VERSION"

COPY pyproject.toml ./
COPY .env ./
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

RUN poetry install --no-root

EXPOSE 8888

CMD ["/entrypoint.sh"]