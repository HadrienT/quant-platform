# Build from the repository root:  docker compose build data-quality
FROM python:3.13.5-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY data_quality/requirements.txt .
RUN pip install -r requirements.txt

COPY common/qp_common ./qp_common
COPY data_quality/dq ./dq

RUN useradd --system --uid 10002 --no-create-home dq
USER dq

CMD ["python", "-m", "dq"]
