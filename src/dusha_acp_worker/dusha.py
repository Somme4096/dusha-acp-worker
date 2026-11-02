from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from .config import DushaConfig


class DushaError(RuntimeError):
    pass


class Dusha:
    """The routes of a dusha hub: https://github.com/Somme4096/dusha/blob/main/docs/api.md"""

    def __init__(self, config: DushaConfig, transport: httpx.AsyncBaseTransport | None = None):
        self.config = config
        token = config.resolved_token()
        self.http = httpx.AsyncClient(
            base_url=config.url.rstrip("/"),
            headers={"X-Companion-Token": token} if token else {},
            timeout=httpx.Timeout(120, connect=10),
            transport=transport,
        )

    # Turns, run by the hub's acp plugin on whichever device the user is at.

    async def turn(
        self, target: dict[str, Any], blocks: list[dict[str, Any]]
    ) -> AsyncIterator[dict[str, Any]]:
        body = {**target, "prompt": blocks}
        timeout = httpx.Timeout(30, read=None)
        async with self.http.stream("POST", "/state/v1/acp/turn", json=body, timeout=timeout) as response:
            if response.status_code >= 400:
                await response.aread()
                raise DushaError(f"the hub answered {response.status_code}: {response.text[:300]}")
            async for line in response.aiter_lines():
                if line.startswith("data:"):
                    yield json.loads(line[5:])

    async def acp(self, operation: str, target: dict[str, Any], **extra: Any) -> dict[str, Any]:
        response = await self.http.post(f"/state/v1/acp/{operation}", json={**target, **extra})
        return self._json(response)

    async def executor(self, device: str = "") -> dict[str, Any]:
        if device:
            response = await self.http.put("/state/v1/acp/executor", json={"device": device})
        else:
            response = await self.http.get("/state/v1/acp/executor")
        return self._json(response)

    def _json(self, response: httpx.Response) -> dict[str, Any]:
        if response.status_code == 422:
            detail = response.json().get("detail")
            raise DushaError(detail if isinstance(detail, str) else str(detail))
        response.raise_for_status()
        return response.json()

    # Proactive events and voice clips.

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
