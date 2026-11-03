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

from .config import Config
from .dusha import Dusha, DushaError

logger = logging.getLogger("dusha_acp_worker")

HELP = """Commands:
/new  start a fresh session
/stop  stop the running turn
/model [spec]  show or switch the model
/plan, /build  switch the agent's mode
/sessions  list stored sessions
/resume <number|id>  continue a stored session
/where  show which device runs the agent
/here <device|auto>  pin the agent to a device, or follow presence
/help  this list"""


class Chat(Protocol):
    name: str
    prefix: str

    async def send(self, channel_id: int, text: str) -> None: ...

    async def send_file(self, channel_id: int, name: str, data: bytes) -> None: ...

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
    task: asyncio.Task[None] | None = None
    busy: bool = False
    listed: list[dict[str, Any]] = field(default_factory=list)
    models: list[str] = field(default_factory=list)


class Bridge:
    def __init__(self, config: Config, chat: Chat, dusha: Dusha):
        self.config = config
        self.chat = chat
        self.dusha = dusha
        self.channels: dict[int, Channel] = {}
        self.state = self._load_state()
        self.clip_lock = asyncio.Lock()
        self.commands: dict[str, Callable[[Channel, str], Awaitable[str]]] = {
            "new": self._new,
            "stop": self._stop,
            "model": self._model,
            "plan": lambda channel, _: self._mode(channel, "plan"),
            "build": lambda channel, _: self._mode(channel, "build"),
            "sessions": self._sessions,
            "resume": self._resume,
            "where": lambda channel, _: self._where(),
            "here": lambda channel, device: self._here(device),
            "help": lambda channel, _: self._help(),
        }

    async def handle(self, channel_id: int, text: str, images: list[Image] | None = None) -> None:
        name, _, rest = text.strip().partition(" ")
        if name.startswith("/") and name[1:].lower() in self.commands and not images:
            await self.chat.send(channel_id, await self.command(channel_id, name[1:].lower(), rest.strip()))
            return
        channel = self._channel(channel_id)
        incoming = Incoming(text, list(images or []))
        if channel.busy and not channel.inbox and await self._steer(channel, incoming):
            return
        channel.inbox.append(incoming)
        channel.wake.set()

    async def command(self, channel_id: int, name: str, argument: str = "") -> str:
        try:
            return await self.commands[name](self._channel(channel_id), argument)
        except Exception as error:
            logger.exception("command /%s failed", name)
            return f"/{name} failed: {error}"

    def models(self, channel_id: int) -> list[str]:
        channel = self.channels.get(channel_id)
        return list(channel.models) if channel else []

    async def run_proactive(self) -> None:
        while True:
            try:
                await self.poll_once()
            except Exception as error:
                logger.warning("proactive poll failed: %s", error)
            await self.send_clips()
            await asyncio.sleep(max(5.0, self.config.dusha.poll_interval_seconds))

    async def poll_once(self) -> None:
        for event in await self.dusha.poll():
            await self._deliver(event)

    async def send_clips(self) -> None:
        async with self.clip_lock:
            try:
                for clip in await self.dusha.clips(self.chat.prefix):
                    channel_id = int(clip["route"].removeprefix(self.chat.prefix))
                    data = base64.b64decode(clip["data"])
                    await self.chat.send_file(channel_id, f"voice.{clip['format']}", data)
                    await self.dusha.clip_sent(clip["id"])
            except Exception as error:
                logger.warning("voice clip delivery failed: %s", error)

    async def close(self) -> None:
        for channel in self.channels.values():
            if channel.task is not None:
                channel.task.cancel()
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
                    await self._turn(channel, self._blocks(batch))
                except Exception as error:
                    logger.exception("turn failed in channel %s", channel.id)
                    await self.chat.send(channel.id, f"The agent failed: {error}")

    async def _steer(self, channel: Channel, incoming: Incoming) -> bool:
        blocks = self._blocks([incoming])
        if not blocks:
            return False
        try:
            steered = await self.dusha.acp("steer", self._target(channel), prompt=blocks)
        except Exception as error:
            logger.warning("could not steer the turn in channel %s, queueing it: %s", channel.id, error)
            return False
        return bool(steered.get("steered"))

    def _blocks(self, batch: list[Incoming]) -> list[dict[str, Any]]:
        text = "\n".join(item.text for item in batch if item.text.strip())
        blocks: list[dict[str, Any]] = [{"type": "text", "text": text}] if text else []
        for item in batch:
            for image in item.images:
                data = base64.b64encode(image.data).decode()
                blocks.append({"type": "image", "mimeType": image.mime_type, "data": data})
        return blocks

    async def _turn(
        self, channel: Channel, blocks: list[dict[str, Any]], proactive: bool = False
    ) -> list[str]:
        if not blocks:
            return []
        target = self._target(channel)
        text = ""
        channel.busy = True
        try:
            async with self.chat.typing(channel.id):
                async for event in self.dusha.turn({**target, "proactive": proactive}, blocks):
                    kind = event.get("event")
                    if kind == "start":
                        self.state["conversations"][str(event["session_id"])] = target["conversation_id"]
                        self._save_state()
                    elif kind == "error":
                        raise DushaError(str(event.get("message") or "the turn failed"))
                    elif kind == "done":
                        text = str(event.get("text") or "")
        finally:
            channel.busy = False
        if text:
            await self.chat.send(channel.id, text)
        await self.send_clips()
        return [text] if text else []

    def _saved(self, channel: Channel) -> dict[str, Any]:
        return self.state["channels"].setdefault(str(channel.id), {})

    def _target(self, channel: Channel) -> dict[str, Any]:
        saved = self._saved(channel)
        if not saved.get("conversation_id"):
            saved["conversation_id"] = f"{self.config.dusha.harness}-{channel.id}-{uuid.uuid4().hex[:8]}"
            self._save_state()
        return {
            "conversation_id": saved["conversation_id"],
            "route": f"{self.chat.prefix}{channel.id}",
            "harness": self.config.dusha.harness,
        }

    # Commands.

    async def _new(self, channel: Channel, _: str) -> str:
        await self._stop(channel, "")
        async with channel.lock:
            channel.inbox.clear()
            if self._saved(channel).get("conversation_id"):
                await self.dusha.acp("reset", self._target(channel))
            self._saved(channel).pop("conversation_id", None)
            self._save_state()
        return "Started a new session."

    async def _stop(self, channel: Channel, _: str) -> str:
        if channel.busy and (await self.dusha.acp("cancel", self._target(channel))).get("cancelled"):
            return "Stopped."
        return "Nothing is running."

    async def _model(self, channel: Channel, spec: str) -> str:
        shown = await self.dusha.acp("config", self._target(channel), model=spec)
        channel.models = list(shown.get("models") or [])
        if spec:
            return f"Model: {spec}"
        if shown.get("model") is None:
            return "This agent offers no model choice."
        listing = "\n".join(f"- {name}" for name in channel.models[:30])
        more = f"\n…and {len(channel.models) - 30} more" if len(channel.models) > 30 else ""
        return f"Model: {shown['model']}\n{listing}{more}"

    async def _mode(self, channel: Channel, mode: str) -> str:
        await self.dusha.acp("config", self._target(channel), mode=mode)
        return f"Mode: {mode}"

    async def _sessions(self, channel: Channel, _: str) -> str:
        listed = await self.dusha.acp("sessions", self._target(channel))
        channel.listed = list(listed.get("sessions") or [])
        if not channel.listed:
            return "No stored sessions."
        lines = []
        for index, item in enumerate(channel.listed, start=1):
            here = " (current)" if item.get("sessionId") == listed.get("current") else ""
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
            self._saved(channel)["conversation_id"] = self.state["conversations"].get(
                session_id, f"{self.config.dusha.harness}-{session_id}"
            )
            self._save_state()
            resumed = await self.dusha.acp("resume", self._target(channel), session_id=session_id)
        if resumed.get("session_id") != session_id:
            return "That session could not be loaded, started a new one."
        return f"Resumed {session_id}."

    async def _where(self) -> str:
        info = await self.dusha.executor()
        if info["pinned"] != "auto":
            return f"Running on {info['device']}, pinned. Use /here auto to follow presence."
        return f"Running on {info['device']}, following presence. Home is {info['home']}."

    async def _here(self, device: str) -> str:
        if not device:
            return "Usage: /here <device|auto>, see /where."
        info = await self.dusha.executor(device)
        if device == "auto":
            return f"Following presence, running on {info['device']}."
        return f"Pinned to {device}, running on {info['device']}."

    async def _help(self) -> str:
        return HELP

    # Proactive.

    async def _deliver(self, event: dict[str, Any]) -> None:
        route = str((event.get("target") or {}).get("route") or "")
        target = route.removeprefix(self.chat.prefix)
        if not route.startswith(self.chat.prefix) or not target.isdigit():
            await self.dusha.ack(event["id"], "failed", error=f"not a {self.chat.name} route: {route!r}")
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
                sent = await self._turn(channel, blocks, proactive=True)
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
