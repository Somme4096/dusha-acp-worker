from __future__ import annotations

import asyncio
import logging
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import discord

from .bridge import Bridge, Image
from .config import Config
from .dusha import Dusha

logger = logging.getLogger("dusha_acp_worker.discord")

LIMIT = 2000


def chunks(text: str, limit: int = LIMIT) -> list[str]:
    parts: list[str] = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut <= 0:
            cut = text.rfind(" ", 0, limit)
        if cut <= 0:
            cut = limit
        parts.append(text[:cut])
        text = text[cut:].lstrip("\n")
    if text:
        parts.append(text)
    return parts


class DiscordChat(discord.Client):
    def __init__(self, config: Config):
        intents = discord.Intents.none()
        intents.dm_messages = True
        intents.message_content = True
        super().__init__(intents=intents)
        self.config = config
        self.bridge = Bridge(config, self, Dusha(config.dusha) if config.dusha.url else None)
        self._proactive: asyncio.Task[None] | None = None

    async def setup_hook(self) -> None:
        self._proactive = asyncio.create_task(self.bridge.run_proactive())

    async def close(self) -> None:
        if self._proactive is not None:
            self._proactive.cancel()
        await self.bridge.close()
        await super().close()

    async def on_ready(self) -> None:
        logger.info("logged in as %s", self.user)

    async def on_message(self, message: discord.Message) -> None:
        if message.guild is not None or message.author.bot:
            return
        allowed = self.config.discord.allowed_user_ids
        if allowed and message.author.id not in allowed:
            return
        images = [
            Image(item.content_type.split(";")[0], await item.read())
            for item in message.attachments
            if (item.content_type or "").startswith("image/")
        ]
        await self.bridge.handle(message.channel.id, message.content, images)

    async def _channel(self, channel_id: int) -> Any:
        return self.get_channel(channel_id) or await self.fetch_channel(channel_id)

    async def send(self, channel_id: int, text: str) -> None:
        channel = await self._channel(channel_id)
        for part in chunks(text):
            await channel.send(part)

    def typing(self, channel_id: int) -> AbstractAsyncContextManager[Any]:
        @asynccontextmanager
        async def typing():
            channel = await self._channel(channel_id)
            async with channel.typing():
                yield

        return typing()
