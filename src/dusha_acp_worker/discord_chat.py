from __future__ import annotations

import asyncio
import io
import logging
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import discord
from discord import app_commands

from .bridge import Bridge, Image
from .config import Config
from .dusha import Dusha

logger = logging.getLogger("dusha_acp_worker.discord")

LIMIT = 2000
COMMANDS = {
    "new": "Start a fresh session",
    "stop": "Stop the running turn",
    "model": "Show the models, or switch to one",
    "plan": "Switch to plan mode",
    "build": "Switch to build mode",
    "sessions": "List stored sessions",
    "resume": "Continue a stored session",
    "where": "Show which device runs the agent",
    "here": "Pin the agent to a device, or auto to follow presence",
    "help": "List the commands",
}


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
    name = "Discord"
    prefix = "discord:"

    def __init__(self, config: Config):
        intents = discord.Intents.none()
        intents.dm_messages = True
        intents.message_content = True
        super().__init__(intents=intents)
        self.config = config
        self.bridge = Bridge(config, self, Dusha(config.dusha))
        self._proactive: asyncio.Task[None] | None = None
        self.tree = app_commands.CommandTree(self)
        for command in self._commands():
            self.tree.add_command(command)

    async def setup_hook(self) -> None:
        self._proactive = asyncio.create_task(self.bridge.run_proactive())
        if self.config.discord.sync_commands:
            synced = await self.tree.sync()
            logger.info("registered slash commands: %s", ", ".join(item.name for item in synced))

    def _commands(self) -> list[app_commands.Command[Any, Any, Any]]:
        async def plain(interaction: discord.Interaction) -> None:
            await self._run(interaction, interaction.command.name, "")

        async def model(interaction: discord.Interaction, spec: str | None = None) -> None:
            await self._run(interaction, "model", spec or "")

        async def resume(interaction: discord.Interaction, session: str) -> None:
            await self._run(interaction, "resume", session)

        async def here(interaction: discord.Interaction, device: str) -> None:
            await self._run(interaction, "here", device)

        callbacks = {"model": model, "resume": resume, "here": here}
        commands = []
        for name, description in COMMANDS.items():
            command = app_commands.Command(
                name=name, description=description, callback=callbacks.get(name, plain)
            )
            command = app_commands.allowed_contexts(guilds=False, dms=True, private_channels=False)(command)
            commands.append(app_commands.allowed_installs(guilds=True, users=True)(command))
        named = {command.name: command for command in commands}
        named["model"].autocomplete("spec")(self._model_names)
        app_commands.describe(spec="A model spec such as provider/model-id")(named["model"])
        app_commands.describe(session="A number from /sessions, or a session id")(named["resume"])
        app_commands.describe(device="A device name from presence, or auto")(named["here"])
        return commands

    async def _model_names(
        self, interaction: discord.Interaction, current: str
    ) -> list[app_commands.Choice[str]]:
        names = [
            name for name in self.bridge.models(interaction.channel_id) if current.lower() in name.lower()
        ]
        return [app_commands.Choice(name=name[:100], value=name) for name in names[:25]]

    async def _run(self, interaction: discord.Interaction, name: str, argument: str) -> None:
        allowed = self.config.discord.allowed_user_ids
        if allowed and interaction.user.id not in allowed:
            await interaction.response.send_message("This companion isn't yours.", ephemeral=True)
            return
        await interaction.response.defer(thinking=True)
        reply = await self.bridge.command(interaction.channel_id, name, argument)
        for part in chunks(reply) or ["Done."]:
            await interaction.followup.send(part)

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

    async def send_file(self, channel_id: int, name: str, data: bytes) -> None:
        channel = await self._channel(channel_id)
        await channel.send(file=discord.File(io.BytesIO(data), filename=name))

    def typing(self, channel_id: int) -> AbstractAsyncContextManager[Any]:
        @asynccontextmanager
        async def typing():
            channel = await self._channel(channel_id)
            async with channel.typing():
                yield

        return typing()
