from __future__ import annotations

from typing import Any

import httpx

from .config import DushaConfig


class Dusha:
    """The proactive routes of a dusha server: https://github.com/Somme4096/dusha/blob/main/docs/api.md"""

    def __init__(self, config: DushaConfig, transport: httpx.AsyncBaseTransport | None = None):
        self.config = config
        token = config.resolved_token()
        self.http = httpx.AsyncClient(
            base_url=config.url.rstrip("/"),
            headers={"X-Companion-Token": token} if token else {},
            timeout=30,
            transport=transport,
        )

    async def poll(self) -> list[dict[str, Any]]:
        response = await self.http.get(
            "/state/v1/proactive/events",
            params={"consumer": self.config.consumer, "harness": self.config.harness, "limit": 1},
        )
        response.raise_for_status()
        return list(response.json().get("events") or [])

    async def ack(self, event_id: str, outcome: str, text: str = "", error: str = "") -> None:
        response = await self.http.post(
            f"/state/v1/proactive/events/{event_id}/ack",
            json={"consumer": self.config.consumer, "outcome": outcome, "text": text, "error": error[:1000]},
        )
        response.raise_for_status()

    async def clips(self, prefix: str) -> list[dict[str, Any]]:
        response = await self.http.get("/state/v1/voice/outbox", params={"prefix": prefix})
        if response.status_code in {404, 503}:
            return []
        response.raise_for_status()
        return list(response.json().get("clips") or [])

    async def clip_sent(self, clip_id: str) -> None:
        response = await self.http.delete(f"/state/v1/voice/outbox/{clip_id}")
        response.raise_for_status()

    async def close(self) -> None:
        await self.http.aclose()
