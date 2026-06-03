import asyncio
import gzip
import json
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger("log-collector.forwarder")


class LogForwarder:
    def __init__(self, loki_url: str = "http://loki:3100", otel_endpoint: str = "otel-collector:4317"):
        self.loki_url = loki_url.rstrip("/")
        self.otel_endpoint = otel_endpoint.rstrip("/")
        self._loki_push_url = f"{self.loki_url}/loki/api/v1/push"
        self._otel_http_url = f"http://{self.otel_endpoint}/v1/logs"
        self._client: httpx.AsyncClient | None = None
        self._backoff_base = 0.5
        self._max_retries = 3

    async def start(self):
        limits = httpx.Limits(max_connections=50, max_keepalive_connections=20)
        timeout = httpx.Timeout(30.0, connect=10.0)
        self._client = httpx.AsyncClient(limits=limits, timeout=timeout, http2=True)
        logger.info("Forwarder initialized: Loki=%s, OTel=%s", self._loki_push_url, self._otel_http_url)

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _retry(self, coro_factory, retries: int = None):
        if retries is None:
            retries = self._max_retries
        last_exc = None
        for attempt in range(retries):
            try:
                return await coro_factory()
            except Exception as exc:
                last_exc = exc
                if attempt < retries - 1:
                    delay = self._backoff_base * (2 ** attempt)
                    logger.warning("Retry %d/%d after %0.1fs: %s", attempt + 1, retries, delay, exc)
                    await asyncio.sleep(delay)
        raise last_exc

    def _compress(self, data: bytes) -> bytes:
        return gzip.compress(data)

    async def forward_to_loki(self, entry: Any) -> dict:
        if not self._client:
            raise RuntimeError("Forwarder not started")

        timestamp = getattr(entry, "timestamp", None) or time.time()
        if isinstance(timestamp, str):
            try:
                timestamp = time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S"))
            except (ValueError, TypeError):
                timestamp = time.time()
        nanos = int(timestamp * 1_000_000_000)

        service = entry.service if hasattr(entry, "service") else entry.get("service", "unknown")
        level = entry.level if hasattr(entry, "level") else entry.get("level", "info")
        message = entry.message if hasattr(entry, "message") else entry.get("message", "")
        metadata = entry.metadata if hasattr(entry, "metadata") else entry.get("metadata", {})

        stream = {
            "stream": {
                "job": f"kirov-{service}",
                "service": service,
                "level": level,
                "source": metadata.get("source", "agent"),
            },
            "values": [
                [str(nanos), json.dumps({"message": message, "metadata": metadata})]
            ],
        }

        payload = {"streams": [stream]}
        body = self._compress(json.dumps(payload).encode("utf-8"))

        async def send():
            resp = await self._client.post(
                self._loki_push_url,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip",
                },
            )
            resp.raise_for_status()
            return resp.json()

        return await self._retry(send)

    async def forward_to_otel(self, entry: Any) -> dict:
        if not self._client:
            raise RuntimeError("Forwarder not started")

        service = entry.service if hasattr(entry, "service") else entry.get("service", "unknown")
        level = entry.level if hasattr(entry, "level") else entry.get("level", "info")
        message = entry.message if hasattr(entry, "message") else entry.get("message", "")
        metadata = entry.metadata if hasattr(entry, "metadata") else entry.get("metadata", {})

        otel_payload = {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": service}},
                            {"key": "service.namespace", "value": {"stringValue": "kirov"}},
                        ]
                    },
                    "scopeLogs": [
                        {
                            "scope": {"name": "kirov-log-collector"},
                            "logRecords": [
                                {
                                    "timeUnixNano": str(int(time.time() * 1_000_000_000)),
                                    "severityText": level.upper(),
                                    "severityNumber": self._severity_number(level),
                                    "body": {"stringValue": message},
                                    "attributes": [
                                        {"key": k, "value": {"stringValue": str(v)}}
                                        for k, v in metadata.items()
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        body = self._compress(json.dumps(otel_payload).encode("utf-8"))

        async def send():
            resp = await self._client.post(
                self._otel_http_url,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip",
                },
            )
            resp.raise_for_status()
            return resp.json()

        return await self._retry(send)

    async def batch_forward(self, entries: list[Any]) -> list[dict]:
        if not self._client:
            raise RuntimeError("Forwarder not started")

        streams: dict[str, list] = {}
        for entry in entries:
            service = entry.service if hasattr(entry, "service") else entry.get("service", "unknown")
            level = entry.level if hasattr(entry, "level") else entry.get("level", "info")
            message = entry.message if hasattr(entry, "message") else entry.get("message", "")
            metadata = entry.metadata if hasattr(entry, "metadata") else entry.get("metadata", {})

            stream_key = f"{service}|{level}"
            if stream_key not in streams:
                streams[stream_key] = {
                    "stream": {
                        "job": f"kirov-{service}",
                        "service": service,
                        "level": level,
                    },
                    "values": [],
                }

            streams[stream_key]["values"].append([
                str(int(time.time() * 1_000_000_000)),
                json.dumps({"message": message, "metadata": metadata}),
            ])

        payload = {"streams": list(streams.values())}
        body = self._compress(json.dumps(payload).encode("utf-8"))

        async def send():
            resp = await self._client.post(
                self._loki_push_url,
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip",
                },
            )
            resp.raise_for_status()
            return resp.json()

        result = await self._retry(send)
        logger.info("Batch forwarded %d entries in %d streams", len(entries), len(streams))
        return result

    @staticmethod
    def _severity_number(level: str) -> int:
        mapping = {
            "debug": 5,
            "info": 9,
            "warn": 13,
            "warning": 13,
            "error": 17,
            "fatal": 21,
            "critical": 21,
        }
        return mapping.get(level.lower(), 9)
