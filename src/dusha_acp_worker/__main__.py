from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import config as configuration


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dusha-acp-worker", description="A Discord DM companion on an ACP agent"
    )
    parser.add_argument("config", nargs="?", default="config.json", type=Path)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        config = configuration.load(args.config)
    except (OSError, ValueError, TypeError) as error:
        print(f"dusha-acp-worker: {args.config}: {error}", file=sys.stderr)
        raise SystemExit(2) from error
    token = config.discord.resolved_token()
    if not token:
        print(f"dusha-acp-worker: set discord.token or ${config.discord.token_env}", file=sys.stderr)
        raise SystemExit(2)
    from .discord_chat import DiscordChat

    DiscordChat(config).run(token, log_handler=None)


if __name__ == "__main__":
    main()
