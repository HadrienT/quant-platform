"""Prometheus endpoint for a consumer. Labels are low-cardinality by rule."""

from prometheus_client import start_http_server


def serve(port: int) -> None:
    """Expose /metrics on 0.0.0.0:port (the container port is never published)."""
    start_http_server(port)
