from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Any, Protocol

from .acp import AcpError, Agent
from .config import Config
from .dusha import Dusha

logger = logging.getLogger("dusha_acp_worker")

ROUTE_PREFIX = "discord:"
HELP = """Commands:
/new  start a fresh session
/stop  stop the running turn
/model [spec]  show or switch the model
/plan, /build  switch the agent's mode
/sessions  list stored sessions
/resume <number|id>  continue a stored session
/help  this list"""


class Chat(Protocol):
    async def send(self, channel_id: int, text: str) -> None: ...

    def typing(self, channel_id: int) -> AbstractAsyncContextManager[Any]: ...


@dataclass
class Image:
    mime_type: str
    data: bytes


@dataclass
class Incoming:
    text: str
    images: list[Image] = field(default_factory=list)


@dataclass
class Channel:
    id: int
    inbox: list[Incoming] = field(default_factory=list)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    agent: Agent | None = None
    task: asyncio.Task[None] | None = None
    listed: list[dict[str, Any]] = field(default_factory=list)


class Bridge:
    def __init__(self, config: Config, chat: Chat, dusha: Dusha | None = None):
        self.config = config
        self.chat = chat
        self.dusha = dusha
        self.channels: dict[int, Channel] = {}
        self.state = self._load_state()
        self.commands: dict[str, Callable[[Channel, str], Awaitable[str]]] = {
            "new": self._new,
            "stop": self._stop,
            "model": self._model,
            "plan": lambda channel, _: self._mode(channel, "plan"),
            "build": lambda channel, _: self._mode(channel, "build"),
            "sessions": self._sessions,
            "resume": self._resume,
            "help": lambda channel, _: self._help(),
        }

    async def handle(self, channel_id: int, text: str, images: list[Image] | None = None) -> None:
        name, _, rest = text.strip().partition(" ")
        if name.startswith("/") and name[1:].lower() in self.commands and not images:
            await self.chat.send(channel_id, await self.command(channel_id, name[1:].lower(), rest.strip()))
            return
        channel = self._channel(channel_id)
        channel.inbox.append(Incoming(text, list(images or [])))
        channel.wake.set()

    async def command(self, channel_id: int, name: str, argument: str = "") -> str:
        try:
            return await self.commands[name](self._channel(channel_id), argument)
        except Exception as error:
            logger.exception("command /%s failed", name)
            return f"/{name} failed: {error}"

    def models(self, channel_id: int) -> list[str]:
        channel = self.channels.get(channel_id)
        option = channel.agent.option("model") if channel and channel.agent else None
        return [str(item.get("value")) for item in (option or {}).get("options") or []]

    async def run_proactive(self) -> None:
        if self.dusha is None:
            return
        while True:
            try:
                await self.poll_once()
            except Exception as error:
                logger.warning("proactive poll failed: %s", error)
            await asyncio.sleep(max(5.0, self.config.dusha.poll_interval_seconds))

    async def poll_once(self) -> None:
        assert self.dusha is not None
        for event in await self.dusha.poll():
            await self._deliver(event)

    async def close(self) -> None:
        for channel in self.channels.values():
            if channel.task is not None:
                channel.task.cancel()
            await self._drop_agent(channel)
        if self.dusha is not None:
            await self.dusha.close()

    # Turns.

    def _channel(self, channel_id: int) -> Channel:
        channel = self.channels.get(channel_id)
        if channel is None:
            channel = self.channels[channel_id] = Channel(channel_id)
            channel.task = asyncio.create_task(self._work(channel))
        return channel

    async def _work(self, channel: Channel) -> None:
        while True:
            await channel.wake.wait()
            await asyncio.sleep(self.config.debounce_seconds)
            channel.wake.clear()
            async with channel.lock:
                batch, channel.inbox = channel.inbox, []
                if not batch:
                    continue
                try:
                    await self._agent(channel)
                    await self._turn(channel, self._blocks(channel, batch))
                except Exception as error:
                    logger.exception("turn failed in channel %s", channel.id)
                    await self.chat.send(channel.id, f"The agent failed: {error}")

    def _blocks(self, channel: Channel, batch: list[Incoming]) -> list[dict[str, Any]]:
        text = "\n".join(item.text for item in batch if item.text.strip())
        blocks: list[dict[str, Any]] = [{"type": "text", "text": text}] if text else []
        capabilities = channel.agent.capabilities if channel.agent else {}
        vision = bool((capabilities.get("promptCapabilities") or {}).get("image"))
        for item in batch:
            for image in item.images if vision else []:
                data = base64.b64encode(image.data).decode()
                blocks.append({"type": "image", "mimeType": image.mime_type, "data": data})
        return blocks

    async def _turn(self, channel: Channel, blocks: list[dict[str, Any]]) -> list[str]:
        agent = await self._agent(channel)
        if not blocks:
            return []
        try:
            async with self.chat.typing(channel.id):
                _, sent = await agent.prompt(blocks, lambda text: self.chat.send(channel.id, text))
        finally:
            if not agent.alive:
                await self._drop_agent(channel)
        return sent

    # Agent lifecycle.

    def _saved(self, channel: Channel) -> dict[str, Any]:
        return self.state["channels"].setdefault(str(channel.id), {})

    async def _agent(self, channel: Channel) -> Agent:
        if channel.agent is not None and channel.agent.alive:
            return channel.agent
        await self._drop_agent(channel)
        saved = self._saved(channel)
        if not saved.get("conversation_id"):
            saved["conversation_id"] = f"{self.config.dusha.harness}-{channel.id}-{uuid.uuid4().hex[:8]}"
        agent = await self._spawn(channel, saved)
        if saved.get("session_id"):
            try:
                await agent.load_session(saved["session_id"])
            except AcpError as error:
                logger.warning("could not load session %s, starting fresh: %s", saved["session_id"], error)
                await agent.close()
                saved.pop("session_id")
                saved["conversation_id"] = f"{self.config.dusha.harness}-{channel.id}-{uuid.uuid4().hex[:8]}"
                agent = await self._spawn(channel, saved)
        if not agent.session_id:
            await agent.new_session()
        saved["session_id"] = agent.session_id
        self.state["conversations"][agent.session_id] = saved["conversation_id"]
        mode = saved.get("mode") or self.config.agent.mode
        if agent.modes and agent.modes.get("currentModeId") != mode:
            await agent.set_mode(mode)
        model = saved.get("model") or self.config.agent.model
        current = (agent.option("model") or {}).get("currentValue")
        if model and current is not None and current != model:
            await agent.set_model(model)
        channel.agent = agent
        self._save_state()
        return agent

    async def _spawn(self, channel: Channel, saved: dict[str, Any]) -> Agent:
        env = {
            **self.config.agent.env,
            "DUSHA_CONVERSATION_ID": saved["conversation_id"],
            "DUSHA_ROUTE": f"{ROUTE_PREFIX}{channel.id}",
            "DUSHA_HARNESS": self.config.dusha.harness,
        }
        agent = Agent(self.config.agent.command, self.config.cwd, env)
        try:
            await agent.start()
        except Exception:
            await agent.close()
            raise
        return agent

    async def _drop_agent(self, channel: Channel) -> None:
        agent, channel.agent = channel.agent, None
        if agent is not None:
            await agent.close()

    async def _ready(self, channel: Channel) -> Agent:
        if channel.agent is not None and channel.agent.alive:
            return channel.agent
        async with channel.lock:
            return await self._agent(channel)

    # Commands.

    async def _new(self, channel: Channel, _: str) -> str:
        await self._stop(channel, "")
        async with channel.lock:
            channel.inbox.clear()
            await self._drop_agent(channel)
            saved = self._saved(channel)
            saved.pop("session_id", None)
            saved.pop("conversation_id", None)
            saved.pop("mode", None)
            self._save_state()
        return "Started a new session."

    async def _stop(self, channel: Channel, _: str) -> str:
        if channel.agent is not None and channel.agent.busy:
            await channel.agent.cancel()
            return "Stopped."
        return "Nothing is running."

    async def _model(self, channel: Channel, spec: str) -> str:
        agent = await self._ready(channel)
        if spec:
            await agent.set_model(spec)
            self._saved(channel)["model"] = spec
            self._save_state()
            return f"Model: {spec}"
        option = agent.option("model")
        if option is None:
            return "This agent offers no model choice."
        names = [str(item.get("value")) for item in option.get("options") or []]
        listing = "\n".join(f"- {name}" for name in names[:30])
        more = f"\n…and {len(names) - 30} more" if len(names) > 30 else ""
        return f"Model: {option.get('currentValue')}\n{listing}{more}"

    async def _mode(self, channel: Channel, mode: str) -> str:
        agent = await self._ready(channel)
        await agent.set_mode(mode)
        self._saved(channel)["mode"] = mode
        self._save_state()
        return f"Mode: {mode}"

    async def _sessions(self, channel: Channel, _: str) -> str:
        agent = await self._ready(channel)
        channel.listed = (await agent.list_sessions())[:15]
        if not channel.listed:
            return "No stored sessions."
        lines = []
        for index, item in enumerate(channel.listed, start=1):
            here = " (current)" if item.get("sessionId") == agent.session_id else ""
            title = item.get("title") or item.get("sessionId")
            lines.append(f"{index}. {title} · {item.get('updatedAt') or ''}{here}")
        return "\n".join(lines) + "\nUse /resume <number>."

    async def _resume(self, channel: Channel, choice: str) -> str:
        if not choice:
            return "Usage: /resume <number|id>, see /sessions."
        session_id = choice
        if choice.isdigit():
            index = int(choice) - 1
            if not 0 <= index < len(channel.listed):
                return "No such session, run /sessions first."
            session_id = str(channel.listed[index]["sessionId"])
        await self._stop(channel, "")
        async with channel.lock:
            channel.inbox.clear()
            await self._drop_agent(channel)
            saved = self._saved(channel)
            saved["session_id"] = session_id
            saved["conversation_id"] = self.state["conversations"].get(
                session_id, f"{self.config.dusha.harness}-{session_id}"
            )
            agent = await self._agent(channel)
        if agent.session_id != session_id:
            return "That session could not be loaded, started a new one."
        return f"Resumed {session_id}."

    async def _help(self) -> str:
        return HELP

    # Proactive.

    async def _deliver(self, event: dict[str, Any]) -> None:
        assert self.dusha is not None
        route = str((event.get("target") or {}).get("route") or "")
        target = route.removeprefix(ROUTE_PREFIX)
        if not route.startswith(ROUTE_PREFIX) or not target.isdigit():
            await self.dusha.ack(event["id"], "failed", error=f"not a Discord route: {route!r}")
            return
        marker = event.get("claim_marker")
        if not marker:
            await self.dusha.ack(
                event["id"], "failed", error="the event has no claim_marker, use better_proactive"
            )
            return
        channel = self._channel(int(target))
        if channel.inbox or channel.lock.locked():
            await self.dusha.ack(event["id"], "release")
            return
        blocks = [{"type": "text", "text": f"{marker}\n{event.get('generation_instruction') or ''}"}]
        async with channel.lock:
            try:
                sent = await self._turn(channel, blocks)
            except Exception as error:
                logger.exception("proactive turn failed in channel %s", channel.id)
                await self.dusha.ack(event["id"], "failed", error=str(error))
                return
        if sent:
            await self.dusha.ack(event["id"], "sent", text="\n\n".join(sent))
        else:
            await self.dusha.ack(event["id"], "failed", error="the agent wrote nothing")

    # State.

    def _load_state(self) -> dict[str, Any]:
        path = self.config.state_path
        state: dict[str, Any] = {}
        with contextlib.suppress(FileNotFoundError):
            state = json.loads(path.read_text(encoding="utf-8"))
        state.setdefault("channels", {})
        state.setdefault("conversations", {})
        return state

    def _save_state(self) -> None:
        path = self.config.state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.state, indent=2), encoding="utf-8")
        temporary.replace(path)
