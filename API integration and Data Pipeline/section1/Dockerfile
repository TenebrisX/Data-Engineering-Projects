FROM python:3.11-slim AS base

WORKDIR /app

ENV PIP_NO_CACHE_DIR=1

COPY pyproject.toml README.md ./

RUN pip install --upgrade pip && pip install poetry && poetry config virtualenvs.create false

COPY src ./src
COPY data ./data

RUN poetry install --no-interaction --no-ansi

ENTRYPOINT ["python", "-m", "src.main"]
