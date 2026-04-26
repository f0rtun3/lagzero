FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY lagzero ./lagzero

RUN pip install --no-cache-dir ".[kafka]"

CMD ["python", "-m", "lagzero.main"]
