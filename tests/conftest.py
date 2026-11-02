from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
from dusha import api
from dusha.config import AppConfig, ExternalPluginConfig

from dusha_acp_worker.bridge import Bridge
from dusha_acp_worker.config import parse
from dusha_acp_worker.dusha import Dusha

FAKE = Path(__file__).with_name("fake_agent.py")
PLUGINS = Path(
    os.environ.get("DUSHA_PLUGINS_DIR") or Path(__file__).resolve().parents[2] / "dusha-plugin-collections"
)


class FakeChat:
    name = "Discord"
    prefix = "discord:"

    def __init__(self):
        self.sent: list[tuple[int, str]] = []
        self.typing_for: list[int] = []
        self.files: list[tuple[int, str, bytes]] = []

    async def send(self, channel_id: int, text: str) -> None:
        self.sent.append((channel_id, text))

    async def send_file(self, channel_id: int, name: str, data: bytes) -> None:
        self.files.append((channel_id, name, data))

    def typing(self, channel_id: int):
        @asynccontextmanager
        async def typing():
            self.typing_for.append(channel_id)
            yield

        return typing()

    def texts(self) -> list[str]:
        return [text for _, text in self.sent]


class FakeDusha:
    """The proactive and voice routes of a dusha hub, as the worker sees them."""

    def __init__(self):
        self.pending: list[dict[str, Any]] = []
        self.acks: list[dict[str, Any]] = []
        self.clips: list[dict[str, Any]] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/state/v1/voice/outbox":
            assert request.url.params["prefix"] == "discord:"
            return httpx.Response(200, json={"clips": self.clips})
        if request.method == "DELETE":
            clip_id = request.url.path.split("/")[-1]
            self.clips = [clip for clip in self.clips if clip["id"] != clip_id]
            return httpx.Response(200, json={"id": clip_id})
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


class Split(httpx.AsyncBaseTransport):
    """The acp routes go to the real hub, the rest to FakeDusha."""

    def __init__(self, fake: FakeDusha):
        self.real = httpx.AsyncHTTPTransport()
        self.fake = httpx.MockTransport(fake.handler)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/state/v1/acp/"):
            return await self.real.handle_async_request(request)
        return await self.fake.handle_async_request(request)

    async def aclose(self) -> None:
        await self.real.aclose()


class Hub:
    """A real dusha hub on a loopback port, running the acp plugin with the fake agent."""

    def __init__(self, data_dir: Path, log: Path):
        if not (PLUGINS / "acp").is_dir():
            pytest.fail(f"the acp plugin is not at {PLUGINS / 'acp'}, set DUSHA_PLUGINS_DIR")
        agent = {"command": [sys.executable, str(FAKE)], "env": {"FAKE_LOG": str(log)}, "mode": "build"}
        options = {"agents": {"fake": agent}, "check_seconds": 3600}
        config = AppConfig(
            data_dir=data_dir,
            plugins_dir=str(PLUGINS),
            plugins={"acp": ExternalPluginConfig(options=options)},
            api_token="secret",
        )
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        self.server = uvicorn.Server(
            uvicorn.Config(api.create_app(config), host="127.0.0.1", port=port, log_level="warning")
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started:
            if time.monotonic() > deadline:
                raise RuntimeError("dusha test hub did not start")
            time.sleep(0.02)

    def close(self) -> None:
        self.server.should_exit = True
        self.thread.join(10)


class Harness:
    def __init__(self, tmp_path: Path):
        self.tmp_path = tmp_path
        self.log = tmp_path / "agent.jsonl"
        self.chat = FakeChat()
        self.dusha = FakeDusha()
        self.hub = Hub(tmp_path / "hub", self.log)
        self.bridge: Bridge | None = None

    def start(self) -> Bridge:
        raw = {
            "dusha": {"url": self.hub.url, "token": "secret", "consumer": "worker"},
            "debounce_seconds": 0.2,
        }
        config = parse(raw, self.tmp_path)
        self.bridge = Bridge(config, self.chat, Dusha(config.dusha, transport=Split(self.dusha)))
        return self.bridge

    async def restart(self) -> Bridge:
        assert self.bridge is not None
        await self.bridge.close()
        return self.start()

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
    harness.hub.close()
