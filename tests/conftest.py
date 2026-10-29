from __future__ import annotations

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest

from dusha_acp_worker.bridge import Bridge
from dusha_acp_worker.config import parse
from dusha_acp_worker.dusha import Dusha

FAKE = Path(__file__).with_name("fake_agent.py")


class FakeChat:
    def __init__(self):
        self.sent: list[tuple[int, str]] = []
        self.typing_for: list[int] = []

    async def send(self, channel_id: int, text: str) -> None:
        self.sent.append((channel_id, text))

    def typing(self, channel_id: int):
        @asynccontextmanager
        async def typing():
            self.typing_for.append(channel_id)
            yield

        return typing()

    def texts(self) -> list[str]:
        return [text for _, text in self.sent]


class FakeDusha:
    """The proactive routes of a dusha server, as the worker sees them."""

    def __init__(self):
        self.pending: list[dict[str, Any]] = []
        self.acks: list[dict[str, Any]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            assert request.url.params["consumer"] == "worker"
            assert request.headers["x-companion-token"] == "secret"
            events, self.pending = self.pending, []
            return httpx.Response(200, json={"events": events})
        event_id = request.url.path.split("/")[-2]
        self.acks.append({"id": event_id, **json.loads(request.content)})
        return httpx.Response(200, json={"id": event_id})

    def event(self, route: str, marker: bool = True) -> dict[str, Any]:
        event_id = f"e{len(self.pending) + len(self.acks)}"
        event = {"id": event_id, "target": {"route": route}, "generation_instruction": "Write to her."}
        if marker:
            event["claim_marker"] = f'<dusha_proactive event="{event_id}"/>'
        self.pending.append(event)
        return event


class Harness:
    def __init__(self, tmp_path: Path):
        self.tmp_path = tmp_path
        self.log = tmp_path / "agent.jsonl"
        self.chat = FakeChat()
        self.dusha = FakeDusha()
        self.bridge: Bridge | None = None

    def start(self, **agent: Any) -> Bridge:
        raw = {
            "agent": {
                "command": [sys.executable, str(FAKE)],
                "cwd": ".",
                "env": {"FAKE_LOG": str(self.log)},
                **agent,
            },
            "dusha": {"url": "http://dusha.test", "token": "secret", "consumer": "worker"},
            "debounce_seconds": 0.2,
        }
        config = parse(raw, self.tmp_path)
        dusha = Dusha(config.dusha, transport=httpx.MockTransport(self.dusha.handler))
        self.bridge = Bridge(config, self.chat, dusha)
        return self.bridge

    async def restart(self, **agent: Any) -> Bridge:
        assert self.bridge is not None
        await self.bridge.close()
        return self.start(**agent)

    def records(self, kind: str | None = None) -> list[dict[str, Any]]:
        if not self.log.exists():
            return []
        rows = [json.loads(line) for line in self.log.read_text().splitlines()]
        return [row for row in rows if kind is None or row["kind"] == kind]

    async def until(self, predicate, timeout: float = 10.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"timed out, chat={self.chat.sent}, agent={self.records()}")
            await asyncio.sleep(0.02)


@pytest.fixture
async def harness(tmp_path):
    harness = Harness(tmp_path)
    yield harness
    if harness.bridge is not None:
        await harness.bridge.close()
