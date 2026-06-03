import asyncio
import logging
import os
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager

import httpx
import orjson
from fastapi import FastAPI, HTTPException, Request
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field

from forwarder import LogForwarder

LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100")
OTEL_ENDPOINT = os.environ.get("OTEL_ENDPOINT", "otel-collector:4317")
PROMETHEUS_PUSHGATEWAY = os.environ.get("PROMETHEUS_PUSHGATEWAY", "http://prometheus:9090")
AGENT_PORT = int(os.environ.get("AGENT_PORT", "9095"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
RATE_LIMIT_REQS = 1000
RATE_LIMIT_WINDOW = 60

logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO))
logger = logging.getLogger("log-collector")

ingest_counter = Counter("log_collector_ingested_total", "Total ingested log entries", ["source"])
forward_counter = Counter("log_collector_forwarded_total", "Total forwarded log entries", ["target"])
error_counter = Counter("log_collector_errors_total", "Total processing errors", ["type"])
buffer_gauge = Gauge("log_collector_buffer_entries", "Current buffer size")
rate_limit_gauge = Gauge("log_collector_rate_limit_remaining", "Rate limit remaining", ["source"])
forward_duration = Histogram("log_collector_forward_duration_seconds", "Forwarding duration", ["target"])

event_buffer = deque(maxlen=1000)
rate_limit_buckets: dict[str, deque] = defaultdict(lambda: deque(maxlen=RATE_LIMIT_REQS))
forwarder: LogForwarder | None = None


def check_rate_limit(source: str) -> bool:
    now = time.time()
    bucket = rate_limit_buckets[source]
    while bucket and bucket[0] < now - RATE_LIMIT_WINDOW:
        bucket.popleft()
    if len(bucket) >= RATE_LIMIT_REQS:
        return False
    bucket.append(now)
    rate_limit_gauge.labels(source=source).set(RATE_LIMIT_REQS - len(bucket))
    return True


class LogEntry(BaseModel):
    service: str
    level: str = "info"
    message: str
    metadata: dict = Field(default_factory=dict)
    timestamp: str | None = None


class MetricsPush(BaseModel):
    service: str
    metrics: list[dict] = Field(default_factory=list)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global forwarder
    forwarder = LogForwarder(loki_url=LOKI_URL, otel_endpoint=OTEL_ENDPOINT)
    await forwarder.start()
    logger.info("Log collector agent started on port %d", AGENT_PORT)
    yield
    await forwarder.close()
    logger.info("Log collector agent shut down")


app = FastAPI(title="Kirov Log Collector", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "log-collector", "buffer_size": len(event_buffer)}


@app.post("/api/v1/logs/ingest")
async def ingest_log(entry: LogEntry, request: Request):
    source = request.headers.get("X-Source", entry.service)
    if not check_rate_limit(source):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    ingest_counter.labels(source=source).inc()

    log_data = orjson.dumps(entry.model_dump(mode="json"))
    event_buffer.append(orjson.loads(log_data))
    buffer_gauge.set(len(event_buffer))

    try:
        with forward_duration.labels(target="loki").time():
            await forwarder.forward_to_loki(entry)
        forward_counter.labels(target="loki").inc()
    except Exception as e:
        error_counter.labels(type="loki_forward").inc()
        logger.error("Failed to forward to Loki: %s", e)

    try:
        with forward_duration.labels(target="otel").time():
            await forwarder.forward_to_otel(entry)
        forward_counter.labels(target="otel").inc()
    except Exception as e:
        error_counter.labels(type="otel_forward").inc()
        logger.warning("Failed to forward to OTel: %s", e)

    return {"status": "accepted", "ingested": True}


@app.post("/api/v1/metrics/push")
async def push_metrics(push: MetricsPush, request: Request):
    source = request.headers.get("X-Source", push.service)
    if not check_rate_limit(source):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    try:
        async with httpx.AsyncClient() as client:
            payload = orjson.dumps([{
                "labels": {"job": push.service, "__name__": m.get("name", "unknown")},
                "samples": [{"value": m.get("value", 0)}]
            } for m in push.metrics])
            resp = await client.post(
                f"{PROMETHEUS_PUSHGATEWAY}/metrics/job/{push.service}",
                content=payload,
                headers={"Content-Type": "application/json"}
            )
            resp.raise_for_status()
        forward_counter.labels(target="prometheus").inc()
    except Exception as e:
        error_counter.labels(type="prometheus_push").inc()
        logger.error("Failed to push metrics: %s", e)
        raise HTTPException(status_code=502, detail="Metrics push failed")

    return {"status": "accepted", "pushed": True}


@app.get("/api/v1/logs/recent")
async def recent_logs(limit: int = 100):
    return {"entries": list(event_buffer)[-limit:]}


@app.get("/metrics")
async def metrics():
    return generate_latest()


@app.get("/api/v1/logs/buffer/stats")
async def buffer_stats():
    return {"buffer_size": len(event_buffer), "capacity": event_buffer.maxlen}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=AGENT_PORT, log_level=LOG_LEVEL.lower())
