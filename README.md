# dusha-acp-worker

A Discord DM companion driven by any [ACP](https://agentclientprotocol.com/) agent, built for [maki](https://github.com/tontinton/maki) and [dusha](https://github.com/Somme4096/dusha). The worker only moves messages. The agent runs in the [acp](https://github.com/Somme4096/dusha-plugin-collections/tree/main/acp) plugin on whichever device you are at, and memory, context, and tools come from dusha's proxy and MCP server, which the agent is pointed at.

```
Discord DM ──► dusha-acp-worker ──SSE──► hub: acp plugin ──► picks a device from presence
                     │                        ├─ hub:    maki acp (home)
                     │                        └─ relay ─► device: acp plugin ─► maki acp
                     └── proactive events, voice clips ◄──► hub
```

The hub runs a turn on a device once you have been active there for the plugin's `switch_after_minutes`, and goes back home when you leave it. Each switch starts a new session, which dusha fills with the latest messages, and offers a `device_switch` proactive candidate so the agent can tell you if it wants to.

A message that arrives while a turn runs steers that turn through `POST /state/v1/acp/steer`. The hub cancels the agent's current prompt and prompts again with your message in the same session, so the turn goes on and its one reply answers both. A hub without that route gets the message as the next turn.

## Run

```sh
uv sync
cp config.example.json config.json
DISCORD_TOKEN=... DUSHA_TOKEN=... uv run dusha-acp-worker config.json
```

Turn on `acp` on the hub, and on every device that should run the agent, with `presence` and `relay` on the hub, see the [hub](https://github.com/Somme4096/dusha-plugin-collections/blob/main/config.hub.example.json) and [device](https://github.com/Somme4096/dusha-plugin-collections/blob/main/config.device.example.json) examples.

The Discord bot needs the Message Content intent. The worker answers DMs only, from `allowed_user_ids` when set.

## Point maki at dusha

Do this on every device that runs `acp`. The plugin starts one agent process per conversation and sets `DUSHA_CONVERSATION_ID`, `DUSHA_ROUTE` (`discord:<channel id>`), and `DUSHA_HARNESS` in its environment. Send them to the [dusha proxy](https://github.com/Somme4096/dusha/blob/main/docs/api.md#proxy) as routing headers from `~/.config/maki/providers.toml`:

```toml
[dusha]
display_name = "dusha"
protocol = "openai"
base_url = "http://127.0.0.1:8766/my-profile"
headers = { X-Conversation-Id = "${DUSHA_CONVERSATION_ID}", X-Companion-Route = "${DUSHA_ROUTE}", X-Harness = "${DUSHA_HARNESS}" }
default_model = "dusha/my-model"
```

Add dusha's tools in `~/.config/maki/mcp.toml`:

```toml
[mcp.dusha]
url = "http://127.0.0.1:8765/mcp"
headers = { X-Companion-Token = "${DUSHA_TOKEN}" }
```

## Config

| Key | Default | Meaning |
| --- | --- | --- |
| `discord.token`, `discord.token_env` | `""`, `DISCORD_TOKEN` | Bot token, inline or from the environment. |
| `discord.allowed_user_ids` | `[]` | Users who may talk to it. Empty lets anyone DM it. |
| `discord.sync_commands` | `true` | Register the slash commands on start. |
| `dusha.url` | | The dusha hub, required. The agent's command, folder, mode, and model are options of its `acp` plugin. |
| `dusha.token`, `dusha.token_env` | `""`, `DUSHA_TOKEN` | The server's API token. |
| `dusha.harness` | `discord` | Harness name, used for proactive polls and conversation ids. Match `X-Harness`. |
| `dusha.consumer` | `dusha-acp-worker` | Consumer name for proactive leases. |
| `dusha.poll_interval_seconds` | `30` | Minimum 5. |
| `debounce_seconds` | `2` | Messages that arrive this close together go to the agent as one turn. |
| `state_file` | `state.json` | Each channel's conversation, and which conversation each session belongs to for `/resume`. |

## Commands

| Command | Does |
| --- | --- |
| `/new` | Starts a fresh session and dusha conversation. |
| `/stop` | Cancels the running turn. |
| `/model [spec]` | Shows the models, or switches. The choice sticks to the device across `/new` and restarts. |
| `/plan`, `/build` | Switches the mode. `/new` goes back to the agent's `mode` option. |
| `/sessions` | Lists stored sessions for the agent's working directory. |
| `/resume <number\|id>` | Continues a stored session. |
| `/where` | Shows which device runs the agent. |
| `/here <device\|auto>` | Pins the agent to a device, or follows presence again. |
| `/help` | Lists these. |

The worker registers these as Discord slash commands on start, which replaces every global command the bot application had, such as ones another bot program registered with the same token. Set `discord.sync_commands` to `false` to leave them alone. Typing a command as plain text works too.

Other text, including unknown `/` words, goes to the agent. Messages sent during a turn wait for the next one.

## Proactive messages

Needs [better_proactive](https://github.com/Somme4096/dusha-plugin-collections/tree/main/better_proactive) on the server, which needs dusha's `claim` hook. The worker polls an event, and when the target channel is idle it sends the event's `claim_marker` and `generation_instruction` to the agent as a turn, on the device you are at. dusha stores that turn as an `event`, not as a user message, so affect and silence stay untouched. The agent writes the message with its own tools and history, the worker posts it, and acknowledges `sent`. A busy channel releases the event, a route that is not `discord:<id>` or an empty reply fails it.

## Voice clips

When [voice](https://github.com/Somme4096/dusha-plugin-collections/tree/main/voice) holds a clip for a `discord:` route, the worker uploads it to that DM as a file after each turn and on every proactive poll, then deletes it from the server. A clip that can't be sent stays there and is retried until voice's `clip_ttl_seconds`.

## Develop

The tests start a real dusha hub with the `acp` plugin from a [dusha-plugin-collections](https://github.com/Somme4096/dusha-plugin-collections) checkout next to this one, or from `DUSHA_PLUGINS_DIR`.

```sh
uv run pytest
uv run ruff check .
```
