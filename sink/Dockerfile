# Build from the repository root:  docker compose build audit-sink
FROM python:3.13.5-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app

WORKDIR /app

# Frozen by pip-compile from requirements.in; a separate layer, so code changes
# do not reinstall the dependencies.
COPY sink/requirements.txt .
RUN pip install -r requirements.txt

COPY common/qp_common ./qp_common
COPY sink/audit_sink ./audit_sink

RUN useradd --system --uid 10001 --no-create-home sink
USER sink

CMD ["python", "-m", "audit_sink"]
