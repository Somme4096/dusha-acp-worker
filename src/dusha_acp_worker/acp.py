from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from collections.abc import Awaitable, Callable
from typing import Any

logger = logging.getLogger("dusha_acp_worker.acp")

PROTOCOL_VERSION = 1
LINE_LIMIT = 64 * 1024 * 1024
ALLOW = ("allow_always", "allow_once")

Segment = Callable[[str], Awaitable[None]]


class AcpError(RuntimeError):
    def __init__(self, error: dict[str, Any]):
        detail = error.get("data")
        message = str(error.get("message") or "ACP error")
        super().__init__(f"{message}: {detail}" if detail else message)
        self.code = error.get("code")


class Agent:
    """One ACP agent process holding one session."""

    def __init__(self, command: list[str], cwd: str, env: dict[str, str]):
        self.command = command
        self.cwd = cwd
        self.env = env
        self.process: asyncio.subprocess.Process | None = None
        self.session_id = ""
        self.modes: dict[str, Any] = {}
        self.config_options: list[dict[str, Any]] = []
        self.capabilities: dict[str, Any] = {}
        self._next_id = 0
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._tasks: list[asyncio.Task[Any]] = []
        self._text: list[str] = []
        self._on_segment: Segment | None = None
        self._sent: list[str] = []

    @property
    def alive(self) -> bool:
        return self.process is not None and self.process.returncode is None

    @property
    def busy(self) -> bool:
        return self._on_segment is not None

    async def start(self) -> None:
        self.process = await asyncio.create_subprocess_exec(
            *self.command,
            cwd=self.cwd,
            env={**os.environ, **self.env},
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=LINE_LIMIT,
        )
        self._tasks = [asyncio.create_task(self._read()), asyncio.create_task(self._drain_stderr())]
        result = await self.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "clientCapabilities": {
                    "fs": {"readTextFile": False, "writeTextFile": False},
                    "terminal": False,
                },
            },
        )
        self.capabilities = result.get("agentCapabilities") or {}

    async def new_session(self) -> None:
        result = await self.request("session/new", {"cwd": self.cwd, "mcpServers": []})
        self.session_id = result["sessionId"]
        self._remember(result)

    async def load_session(self, session_id: str) -> None:
        result = await self.request(
            "session/load", {"sessionId": session_id, "cwd": self.cwd, "mcpServers": []}
        )
        self.session_id = session_id
        self._remember(result or {})

    async def list_sessions(self) -> list[dict[str, Any]]:
        result = await self.request("session/list", {"cwd": self.cwd})
        return list(result.get("sessions") or [])

    async def set_mode(self, mode: str) -> None:
        await self.request("session/set_mode", {"sessionId": self.session_id, "modeId": mode})
        self.modes = {**self.modes, "currentModeId": mode}

    async def set_model(self, model: str) -> None:
        result = await self.request(
            "session/set_config_option",
            {"sessionId": self.session_id, "configId": "model", "value": model},
        )
        self._merge_options(result.get("configOptions") or [])

    def option(self, option_id: str) -> dict[str, Any] | None:
        return next((item for item in self.config_options if item.get("id") == option_id), None)

    async def prompt(self, blocks: list[dict[str, Any]], on_segment: Segment) -> tuple[str, list[str]]:
        """Runs one turn. Text between tool calls reaches `on_segment` as one message each."""
        if self.busy:
            raise RuntimeError("a turn is already running")
        self._text, self._sent, self._on_segment = [], [], on_segment
        try:
            result = await self.request("session/prompt", {"sessionId": self.session_id, "prompt": blocks})
            await self._flush()
            return str(result.get("stopReason") or ""), list(self._sent)
        finally:
            self._on_segment = None

    async def cancel(self) -> None:
        if self.busy:
            await self._write(
                {"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": self.session_id}}
            )

    async def close(self) -> None:
        process = self.process
        if process is not None and process.returncode is None:
            with contextlib.suppress(Exception):
                process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), 3)
            except TimeoutError:
                process.kill()
                await process.wait()
        for task in self._tasks:
            task.cancel()
        self._fail_pending(RuntimeError("agent closed"))

    async def request(self, method: str, params: dict[str, Any]) -> Any:
        if not self.alive:
            raise RuntimeError("agent is not running")
        self._next_id += 1
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[self._next_id] = future
        await self._write({"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params})
        return await future

    async def _write(self, message: dict[str, Any]) -> None:
        assert self.process is not None
        self.process.stdin.write(json.dumps(message).encode() + b"\n")
        await self.process.stdin.drain()

    async def _read(self) -> None:
        assert self.process is not None
        try:
            while line := await self.process.stdout.readline():
                try:
                    message = json.loads(line)
                except ValueError:
                    logger.warning("agent wrote a line that is not JSON: %r", line[:200])
                    continue
                await self._dispatch(message)
        finally:
            self._fail_pending(RuntimeError("agent exited"))

    async def _drain_stderr(self) -> None:
        assert self.process is not None
        while line := await self.process.stderr.readline():
            logger.debug("agent: %s", line.decode(errors="replace").rstrip())

    async def _dispatch(self, message: dict[str, Any]) -> None:
        if "method" not in message:
            future = self._pending.pop(message.get("id"), None)
            if future is None or future.done():
                return
            if "error" in message:
                future.set_exception(AcpError(message["error"]))
            else:
                future.set_result(message.get("result"))
            return
        method, params = message["method"], message.get("params") or {}
        if "id" in message:
            await self._answer(message["id"], method, params)
        elif method == "session/update":
            await self._update(params.get("update") or {})

    async def _answer(self, request_id: Any, method: str, params: dict[str, Any]) -> None:
        if method == "session/request_permission":
            options = params.get("options") or []
            pick = next(
                (item for kind in ALLOW for item in options if item.get("kind") == kind),
                None,
            )
            outcome = (
                {"outcome": "selected", "optionId": pick["optionId"]} if pick else {"outcome": "cancelled"}
            )
            await self._write({"jsonrpc": "2.0", "id": request_id, "result": {"outcome": outcome}})
            return
        error = {"code": -32601, "message": f"method not supported: {method}"}
        await self._write({"jsonrpc": "2.0", "id": request_id, "error": error})

    async def _update(self, update: dict[str, Any]) -> None:
        kind = update.get("sessionUpdate")
        if kind == "config_option_update":
            self._merge_options(update.get("configOptions") or [])
        elif kind == "current_mode_update":
            self.modes = {**self.modes, "currentModeId": update.get("currentModeId")}
        if not self.busy:
            return
        if kind == "agent_message_chunk":
            content = update.get("content") or {}
            if content.get("type") == "text":
                self._text.append(str(content.get("text") or ""))
        elif kind == "tool_call":
            await self._flush()

    async def _flush(self) -> None:
        text = "".join(self._text).strip()
        self._text = []
        if text and self._on_segment is not None:
            self._sent.append(text)
            await self._on_segment(text)

    def _remember(self, result: dict[str, Any]) -> None:
        if result.get("modes"):
            self.modes = result["modes"]
        self._merge_options(result.get("configOptions") or [])

    def _merge_options(self, options: list[dict[str, Any]]) -> None:
        known = {item.get("id"): item for item in self.config_options}
        known.update({item.get("id"): item for item in options})
        self.config_options = list(known.values())

    def _fail_pending(self, error: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
