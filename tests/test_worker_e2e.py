from __future__ import annotations

import asyncio
import json

from dusha_acp_worker.bridge import Image

DM = 4242


async def test_a_burst_of_messages_becomes_one_turn_with_the_routing_env(harness):
    bridge = harness.start()
    await bridge.handle(DM, "hey")
    await bridge.handle(DM, "are you there")
    await harness.until(lambda: harness.chat.sent)
    [prompt] = harness.records("prompt")
    assert prompt["text"] == "hey\nare you there"
    assert prompt["mode"] == "build"
    env = prompt["env"]
    assert env["DUSHA_ROUTE"] == f"discord:{DM}"
    assert env["DUSHA_HARNESS"] == "discord"
    assert env["DUSHA_CONVERSATION_ID"].startswith(f"discord-{DM}-")
    assert harness.chat.sent == [(DM, "echo: hey\nare you there")]
    assert harness.chat.typing_for == [DM]


async def test_only_the_text_after_the_last_tool_call_is_sent(harness):
    bridge = harness.start()
    await bridge.handle(DM, "use a tool")
    await harness.until(lambda: harness.chat.sent)
    await asyncio.sleep(0.2)
    assert harness.chat.texts() == ["found it"]


async def test_images_reach_the_agent_and_permissions_are_allowed(harness):
    bridge = harness.start()
    await bridge.handle(DM, "look", [Image("image/png", b"\x89PNG")])
    await harness.until(lambda: harness.chat.sent)
    await bridge.handle(DM, "perm please")
    await harness.until(lambda: len(harness.chat.sent) == 2)
    assert harness.records("prompt")[0]["images"] == ["image/png"]
    assert harness.chat.texts()[1] == "permission yes-always"


async def test_new_starts_a_fresh_session_and_conversation(harness):
    bridge = harness.start()
    await bridge.handle(DM, "first")
    await harness.until(lambda: harness.chat.sent)
    await bridge.handle(DM, "/new")
    await bridge.handle(DM, "second")
    await harness.until(lambda: len(harness.chat.sent) == 3)
    first, second = harness.records("prompt")
    assert harness.chat.texts()[1] == "Started a new session."
    assert first["session"] != second["session"]
    assert first["env"]["DUSHA_CONVERSATION_ID"] != second["env"]["DUSHA_CONVERSATION_ID"]
    assert first["pid"] != second["pid"]


async def test_stop_cancels_the_running_turn(harness):
    bridge = harness.start()
    await bridge.handle(DM, "be slow")
    await harness.until(lambda: harness.records("prompt"))
    await bridge.handle(DM, "/stop")
    await harness.until(lambda: "thinking" in harness.chat.texts())
    assert "Stopped." in harness.chat.texts()
    await bridge.handle(DM, "/stop")
    await harness.until(lambda: harness.chat.texts()[-1] == "Nothing is running.")


async def test_a_restart_loads_the_saved_session_with_its_model_and_mode(harness):
    bridge = harness.start()
    await bridge.handle(DM, "/model")
    assert harness.chat.texts()[-1] == "Model: fake/small\n- fake/small\n- fake/large"
    await bridge.handle(DM, "/model fake/large")
    await bridge.handle(DM, "/plan")
    await bridge.handle(DM, "/model fake/missing")
    assert harness.chat.texts()[1:] == ["Model: fake/large", "Mode: plan", "/model failed: unknown model"]
    [new] = harness.records("new")

    bridge = await harness.restart()
    await bridge.handle(DM, "again")
    await harness.until(lambda: harness.records("prompt"))
    [load] = harness.records("load")
    [prompt] = harness.records("prompt")
    assert load["session"] == new["session"] == prompt["session"]
    assert prompt["model"] == "fake/large" and prompt["mode"] == "plan"
    state = json.loads((harness.tmp_path / "state.json").read_text())
    assert state["conversations"][new["session"]] == prompt["env"]["DUSHA_CONVERSATION_ID"]
    # Replayed history during load never reaches the chat.
    await harness.until(lambda: len(harness.chat.sent) == 5)
    assert "replayed history" not in harness.chat.texts()


async def test_sessions_and_resume_switch_back_to_an_earlier_conversation(harness):
    bridge = harness.start()
    await bridge.handle(DM, "one")
    await harness.until(lambda: len(harness.chat.sent) == 1)
    await bridge.handle(DM, "/new")
    await bridge.handle(DM, "two")
    await harness.until(lambda: len(harness.chat.sent) == 3)
    first, second = harness.records("prompt")
    await bridge.handle(DM, "/sessions")
    listing = harness.chat.texts()[-1]
    assert listing.splitlines()[0].endswith("(current)")
    assert "2. chat " + first["session"][:4] in listing
    await bridge.handle(DM, "/resume 2")
    assert harness.chat.texts()[-1] == f"Resumed {first['session']}."
    await bridge.handle(DM, "three")
    await harness.until(lambda: len(harness.records("prompt")) == 3)
    third = harness.records("prompt")[-1]
    assert third["session"] == first["session"]
    assert third["env"]["DUSHA_CONVERSATION_ID"] == first["env"]["DUSHA_CONVERSATION_ID"]
    await bridge.handle(DM, "/resume 9")
    assert harness.chat.texts()[-1] == "No such session, run /sessions first."


async def test_a_crashed_agent_is_reported_and_respawned(harness):
    bridge = harness.start()
    await bridge.handle(DM, "crash now")
    await harness.until(lambda: harness.chat.sent)
    assert harness.chat.texts()[0].startswith("The agent failed:")
    await bridge.handle(DM, "hello")
    await harness.until(lambda: len(harness.chat.sent) == 2)
    assert harness.chat.texts()[1] == "echo: hello"
    assert len({row["pid"] for row in harness.records("prompt")}) == 2


async def test_a_proactive_event_is_written_by_the_agent_with_the_claim_marker(harness):
    bridge = harness.start()
    event = harness.dusha.event(f"discord:{DM}")
    await bridge.poll_once()
    [prompt] = harness.records("prompt")
    assert prompt["text"] == f"{event['claim_marker']}\nWrite to her."
    assert harness.chat.sent == [(DM, f"echo: {prompt['text']}")]
    assert harness.dusha.acks == [
        {
            "id": event["id"],
            "consumer": "worker",
            "outcome": "sent",
            "text": f"echo: {prompt['text']}",
            "error": "",
        }
    ]


async def test_proactive_events_that_cannot_be_written_now_go_back(harness):
    bridge = harness.start()
    harness.dusha.event("telegram:1")
    harness.dusha.event(f"discord:{DM}", marker=False)
    await bridge.poll_once()
    await bridge.poll_once()
    await bridge.handle(DM, "be slow")
    await harness.until(lambda: harness.records("prompt"))
    harness.dusha.event(f"discord:{DM}")
    await bridge.poll_once()
    await bridge.handle(DM, "/stop")
    outcomes = [(ack["outcome"], ack["error"]) for ack in harness.dusha.acks]
    assert outcomes == [
        ("failed", "not a Discord route: 'telegram:1'"),
        ("failed", "the event has no claim_marker, use better_proactive"),
        ("release", ""),
    ]
    await harness.until(lambda: "thinking" in harness.chat.texts())


async def test_a_proactive_turn_with_no_words_fails(harness):
    bridge = harness.start()
    event = harness.dusha.event(f"discord:{DM}")
    event["generation_instruction"] = "stay silent"
    await bridge.poll_once()
    assert harness.dusha.acks[0]["outcome"] == "failed"
    assert harness.dusha.acks[0]["error"] == "the agent wrote nothing"
    assert harness.chat.sent == []


async def test_messages_during_a_turn_wait_for_the_next_one(harness):
    bridge = harness.start()
    await bridge.handle(DM, "be slow")
    await harness.until(lambda: harness.records("prompt"))
    await bridge.handle(DM, "later")
    await asyncio.sleep(0.4)
    assert len(harness.records("prompt")) == 1
    await bridge.handle(DM, "/stop")
    await harness.until(lambda: len(harness.records("prompt")) == 2)
    assert harness.records("prompt")[1]["text"] == "later"
