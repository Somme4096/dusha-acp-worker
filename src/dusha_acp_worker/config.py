from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any


@dataclass
class DiscordConfig:
    token: str = ""
    token_env: str = "DISCORD_TOKEN"
    allowed_user_ids: list[int] = field(default_factory=list)
    sync_commands: bool = True

    def resolved_token(self) -> str:
        return self.token or os.environ.get(self.token_env, "")


@dataclass
class DushaConfig:
    url: str = ""
    token: str = ""
    token_env: str = "DUSHA_TOKEN"
    harness: str = "discord"
    consumer: str = "dusha-acp-worker"
    poll_interval_seconds: float = 30

    def resolved_token(self) -> str:
        return self.token or os.environ.get(self.token_env, "")


@dataclass
class Config:
    discord: DiscordConfig = field(default_factory=DiscordConfig)
    dusha: DushaConfig = field(default_factory=DushaConfig)
    debounce_seconds: float = 2.0
    state_file: str = "state.json"
    base_dir: Path = field(default_factory=Path.cwd)

    @property
    def state_path(self) -> Path:
        return self.base_dir / Path(self.state_file).expanduser()


def _section(kind: type, raw: Any, name: str) -> Any:
    if not isinstance(raw, dict):
        raise ValueError(f"'{name}' must be an object")
    known = {item.name for item in fields(kind)}
    unknown = sorted(set(raw) - known)
    if unknown:
        raise ValueError(f"unknown '{name}' field(s): {unknown}")
    return kind(**raw)


def parse(raw: dict[str, Any], base_dir: Path) -> Config:
    raw = dict(raw)
    if "agent" in raw:
        raise ValueError("'agent' moved to the acp plugin of dusha-plugin-collections, see the README")
    sections = {"discord": DiscordConfig, "dusha": DushaConfig}
    parts = {name: _section(kind, raw.pop(name, {}), name) for name, kind in sections.items()}
    config = _section(Config, raw, "config")
    for name, value in parts.items():
        setattr(config, name, value)
    config.base_dir = base_dir
    if not config.dusha.url:
        raise ValueError("'dusha.url' is required, it runs the agent")
    return config


def load(path: Path) -> Config:
    return parse(json.loads(path.read_text(encoding="utf-8")), path.resolve().parent)
