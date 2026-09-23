import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.smart_villa.bridge import (
    HeartbeatLost,
    ReconciliationRequired,
    SmartVillaBridge,
    backoff_seconds,
)
from custom_components.smart_villa.const import HEARTBEAT_SECONDS, INTEGRATION_VERSION, MAX_BACKOFF_SECONDS
from custom_components.smart_villa.diagnostics import async_get_config_entry_diagnostics


def test_command_mapping_and_door_exclusion():
    assert SmartVillaBridge._service_call("SWITCH_ON", "light.room", {}) == ("turn_on", {"entity_id": "light.room"})
    assert SmartVillaBridge._service_call("LIGHT_SET_BRIGHTNESS", "light.room", {"brightness": 42}) == (
        "turn_on",
        {"entity_id": "light.room", "brightness_pct": 42},
    )


async def test_expired_command_gets_explicit_negative_ack():
    bridge = object.__new__(SmartVillaBridge)
    bridge.hass = MagicMock()
    bridge.hass.services.async_call = AsyncMock()
    bridge.sequence = 0
    bridge.queue = __import__("asyncio").Queue()
    bridge.status = {}
    bridge._websocket = None
    websocket = MagicMock()
    websocket.send_json = AsyncMock()
    await bridge._handle_command(
        websocket,
        {
            "command_id": "command-1",
            "capability": "SWITCH_ON",
            "entity_id": "light.room",
            "params": {},
            "expires_at": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        },
    )
    payload = bridge.queue.get_nowait()
    assert payload["type"] == "command_ack"
    assert payload["success"] is False
    assert payload["error_code"] == "COMMAND_EXPIRED"


async def test_lock_command_is_rejected_without_service_call():
    bridge = object.__new__(SmartVillaBridge)
    bridge.hass = MagicMock()
    bridge.hass.services.async_call = AsyncMock()
    bridge.sequence = 0
    bridge.queue = __import__("asyncio").Queue()
    bridge.status = {}
    bridge._websocket = None
    websocket = MagicMock()
    websocket.send_json = AsyncMock()
    await bridge._handle_command(
        websocket,
        {
            "command_id": "command-1",
            "capability": "SWITCH_ON",
            "entity_id": "lock.front",
            "params": {},
            "expires_at": (datetime.now(UTC) + timedelta(seconds=2)).isoformat(),
        },
    )
    assert bridge.queue.get_nowait()["error_code"] == "TTLOCK_ONLY"
    bridge.hass.services.async_call.assert_not_awaited()


async def test_transient_disconnect_reconnects_with_backoff():
    bridge = object.__new__(SmartVillaBridge)
    bridge._stopping = False
    bridge._session_authenticated = False
    bridge.status = {"connected": False, "last_error": None, "reconnects": 0}
    bridge.hass = MagicMock()
    attempts = 0

    async def connect():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("network restart")
        bridge._stopping = True

    bridge._connected_session = connect
    with (
        patch("custom_components.smart_villa.bridge.asyncio.sleep", AsyncMock()) as sleep,
        patch("custom_components.smart_villa.bridge.async_create_issue"),
    ):
        await bridge._run()
    assert attempts == 2
    assert bridge.status["reconnects"] == 1
    sleep.assert_awaited_once()


def make_bridge() -> SmartVillaBridge:
    bridge = object.__new__(SmartVillaBridge)
    bridge.hass = MagicMock()
    bridge.entry = MagicMock()
    bridge.sequence = 0
    bridge.queue = asyncio.Queue(maxsize=1000)
    bridge.status = {"connected": False, "last_error": None, "reconnects": 0}
    bridge._websocket = None
    bridge._stopping = False
    bridge._unsubscribers = []
    bridge._session_authenticated = False
    bridge._last_inbound = __import__("time").monotonic()
    return bridge


async def test_burst_state_events_and_heartbeats_keep_monotonic_sequences():
    bridge = make_bridge()

    async def emit(index: int) -> None:
        state = SimpleNamespace(
            entity_id=f"light.synthetic_{index}",
            domain="light",
            state="on" if index % 2 else "off",
            attributes={},
            last_changed=datetime.now(UTC),
            last_updated=datetime.now(UTC),
        )
        await bridge._state_changed(SimpleNamespace(data={"new_state": state}))
        if index % 10 == 0:
            bridge._enqueue(bridge._message("heartbeat"))

    await asyncio.gather(*(emit(index) for index in range(50)))
    messages = []
    while not bridge.queue.empty():
        messages.append(bridge.queue.get_nowait())
    assert [message["seq"] for message in messages] == list(range(1, len(messages) + 1))
    assert sum(message["type"] == "state_changed" for message in messages) == 50
    assert sum(message["type"] == "heartbeat" for message in messages) == 5


async def test_out_of_order_closes_socket_and_requests_reconnect():
    bridge = make_bridge()
    websocket = MagicMock()
    websocket.close = AsyncMock()

    with pytest.raises(ReconciliationRequired):
        await bridge._handle_server_message(
            websocket,
            {
                "type": "error",
                "code": "OUT_OF_ORDER",
                "expected_sequence": 30,
                "received_sequence": 31,
                "reconciliation_required": True,
            },
        )

    websocket.close.assert_awaited_once_with(code=4009, message=b"reconciliation required")
    assert bridge.status["sequence_error"] == {
        "category": "OUT_OF_ORDER",
        "expected_sequence": 30,
        "received_sequence": 31,
    }


def test_reconnect_clears_stale_queue_and_sends_fresh_snapshots():
    bridge = make_bridge()
    bridge.sequence = 91
    bridge.queue.put_nowait({"type": "state_changed", "seq": 91, "event": {"state": "redacted"}})
    bridge._registry_snapshot = MagicMock(return_value=[{"entity_id": "light.synthetic"}])
    bridge._state_snapshot = MagicMock(return_value=[{"entity_id": "light.synthetic", "state": "on"}])

    bridge._queue_reconciliation_snapshots(30)

    registry = bridge.queue.get_nowait()
    state = bridge.queue.get_nowait()
    assert (registry["type"], registry["seq"]) == ("registry_snapshot", 30)
    assert (state["type"], state["seq"]) == ("state_snapshot", 31)
    assert bridge.queue.empty()


async def test_shutdown_cleans_up_listener_socket_and_background_task():
    bridge = make_bridge()
    unsubscribe = MagicMock()
    bridge._unsubscribers = [unsubscribe]
    bridge.store = MagicMock()
    bridge.store.async_save = AsyncMock()
    bridge._websocket = MagicMock()
    bridge._websocket.close = AsyncMock()
    bridge.task = asyncio.create_task(asyncio.sleep(60))

    await bridge.async_stop()

    unsubscribe.assert_called_once_with()
    bridge._websocket.close.assert_awaited_once_with(code=1001, message=b"integration unload")
    assert bridge.task.cancelled()
    bridge.store.async_save.assert_awaited_once_with({"sequence": 0})


async def test_sender_connection_error_propagates_to_session_supervisor():
    bridge = make_bridge()
    bridge.store = MagicMock()
    bridge.store.async_save = AsyncMock()
    bridge.queue.put_nowait({"type": "heartbeat", "seq": 1})
    websocket = MagicMock()
    websocket.send_json = AsyncMock(side_effect=ConnectionError("socket closed"))

    with pytest.raises(ConnectionError, match="socket closed"):
        await bridge._sender(websocket)

    await asyncio.wait_for(bridge.queue.join(), timeout=0.1)


async def test_diagnostics_expose_only_safe_bridge_status():
    bridge = make_bridge()
    bridge.status.update(
        {
            "sequence_error": {"category": "OUT_OF_ORDER", "expected_sequence": 3, "received_sequence": 4},
            "credential": "must-not-appear",
            "entity_state": "must-not-appear",
        }
    )
    bridge.entry.entry_id = "entry-1"
    bridge.entry.data = {"credential": "must-not-appear", "installation_id": "installation-1"}
    bridge.entry.options = {}
    bridge.hass.data = {"smart_villa": {"entry-1": bridge}}

    diagnostics = await async_get_config_entry_diagnostics(bridge.hass, bridge.entry)

    assert "credential" not in diagnostics["status"]
    assert "entity_state" not in diagnostics["status"]
    assert diagnostics["entry"]["credential"] != "must-not-appear"
    assert diagnostics["status"]["sequence_error"]["category"] == "OUT_OF_ORDER"


def test_manifest_and_upgrade_notes_use_the_next_version():
    root = Path(__file__).parents[1]
    manifest = json.loads((root / "custom_components/smart_villa/manifest.json").read_text())
    releases = (root / "RELEASES.md").read_text()
    assert manifest["version"] == INTEGRATION_VERSION == "1.0.2"
    assert 'version = "1.0.2"' in (root / "pyproject.toml").read_text()
    assert "## 1.0.2" in releases
    assert "Intended release tag: `v1.0.2`" in releases


# ── v1.0.2 reconnect (incident 2026-09-23) ─────────────────────────────────────────────────────────────────────


def test_backoff_schedule_is_1_2_5_15_then_capped_at_30():
    assert [backoff_seconds(n) for n in range(7)] == [1.0, 2.0, 5.0, 15.0, 30.0, 30.0, 30.0]
    assert backoff_seconds(3, jitter=0.7) == 15.7
    assert backoff_seconds(50) == MAX_BACKOFF_SECONDS


async def test_backoff_resets_after_an_authenticated_session_dropped():
    """1.0.1 bug: backoff only reset when the session returned normally, which never happens → 300 s per drop."""
    bridge = object.__new__(SmartVillaBridge)
    bridge._stopping = False
    bridge._session_authenticated = False
    bridge.status = {"connected": False, "last_error": None, "reconnects": 0}
    bridge.hass = MagicMock()
    attempt = 0

    async def session():
        nonlocal attempt
        attempt += 1
        if attempt <= 4:
            # four sessions that authenticated and then dropped (Caddy restart ×4)
            bridge._session_authenticated = True
            raise ConnectionError("bridge session ended")
        if attempt <= 6:
            # then two failures to even establish (server down)
            raise ConnectionError("connect failed")
        bridge._stopping = True

    bridge._connected_session = session
    with (
        patch("custom_components.smart_villa.bridge.asyncio.sleep", AsyncMock()) as sleep,
        patch("custom_components.smart_villa.bridge.async_create_issue"),
        patch("custom_components.smart_villa.bridge.random.uniform", return_value=0.0),
    ):
        await bridge._run()
    delays = [call.args[0] for call in sleep.await_args_list]
    assert delays == [1.0, 1.0, 1.0, 1.0, 2.0, 5.0]   # drops after auth restart at 1 s; connect failures escalate
    assert bridge.status["reconnects"] == 6
    assert bridge.status["consecutive_failures"] == 2


async def test_heartbeat_watchdog_ends_session_when_server_goes_silent():
    bridge = make_bridge()
    bridge.entry.options = {"heartbeat_seconds": 15}
    bridge._last_inbound = __import__("time").monotonic() - 31   # > 2 intervals without any server frame
    with patch("custom_components.smart_villa.bridge.asyncio.sleep", AsyncMock()):
        with pytest.raises(HeartbeatLost):
            await bridge._heartbeat()
    assert bridge.status["last_error"] == "heartbeat_ack_missing"
    assert bridge.queue.empty()   # no further heartbeat is queued on a dead socket


async def test_heartbeat_keeps_going_while_server_frames_arrive():
    bridge = make_bridge()
    bridge.entry.options = {"heartbeat_seconds": HEARTBEAT_SECONDS}
    calls = 0

    async def sleep(_seconds):
        nonlocal calls
        calls += 1
        bridge._last_inbound = __import__("time").monotonic()  # a server ack arrived during the interval
        if calls == 3:
            raise asyncio.CancelledError

    with patch("custom_components.smart_villa.bridge.asyncio.sleep", sleep):
        with pytest.raises(asyncio.CancelledError):
            await bridge._heartbeat()
    assert bridge.queue.qsize() == 2
    assert all(bridge.queue.get_nowait()["type"] == "heartbeat" for _ in range(2))


async def test_receiver_records_inbound_time_and_ends_on_close():
    bridge = make_bridge()
    bridge._last_inbound = 0.0
    text = SimpleNamespace(type=__import__("aiohttp").WSMsgType.TEXT, json=lambda: {"type": "ack", "ref_id": "x"})
    closed = SimpleNamespace(type=__import__("aiohttp").WSMsgType.CLOSED)

    class FakeWs:
        def __init__(self, frames):
            self._frames = frames

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._frames:
                raise StopAsyncIteration
            return self._frames.pop(0)

    await bridge._receiver(FakeWs([text, closed, text]))   # returns at CLOSED, never sees the third frame
    assert bridge._last_inbound > 0.0


async def test_service_call_exception_still_sends_negative_ack():
    bridge = make_bridge()
    bridge.hass.services.async_call = AsyncMock(side_effect=RuntimeError("entity unavailable"))
    await bridge._handle_command(
        MagicMock(),
        {
            "command_id": "command-9",
            "capability": "SWITCH_OFF",
            "entity_id": "light.room",
            "params": {},
            "expires_at": (datetime.now(UTC) + timedelta(seconds=2)).isoformat(),
        },
    )
    ack = bridge.queue.get_nowait()
    assert (ack["type"], ack["command_id"], ack["success"]) == ("command_ack", "command-9", False)
    assert ack["error_code"] == "SERVICE_CALL_FAILED"
    assert "entity unavailable" not in json.dumps(ack)   # category only, never the exception text


async def test_cancellation_during_command_is_not_masked_as_failure():
    bridge = make_bridge()
    bridge.hass.services.async_call = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await bridge._handle_command(
            MagicMock(),
            {
                "command_id": "command-10",
                "capability": "SWITCH_ON",
                "entity_id": "light.room",
                "params": {},
                "expires_at": (datetime.now(UTC) + timedelta(seconds=2)).isoformat(),
            },
        )
    assert bridge.queue.empty()


def test_full_queue_never_drops_a_command_ack():
    bridge = make_bridge()
    bridge.queue = asyncio.Queue(maxsize=2)
    bridge.hass.async_create_task = MagicMock()
    bridge._websocket = MagicMock()
    bridge._websocket.close = MagicMock(return_value=None)
    bridge._enqueue(bridge._message("state_changed", event={"entity_id": "light.a"}))
    bridge._enqueue(bridge._message("state_changed", event={"entity_id": "light.b"}))
    bridge._enqueue(bridge._message("state_changed", event={"entity_id": "light.c"}))   # dropped: overflow
    assert bridge.status["last_error"] == "outbound_queue_overflow"
    bridge._enqueue(bridge._message("command_ack", command_id="c1", success=True))     # makes room, kept
    kinds = [bridge.queue.get_nowait()["type"] for _ in range(2)]
    assert kinds == ["state_changed", "command_ack"]


def test_reconnect_sends_exactly_one_registry_and_one_state_snapshot_with_next_sequence():
    bridge = make_bridge()
    bridge.sequence = 15_260
    for seq in (15_258, 15_259, 15_260):
        bridge.queue.put_nowait({"type": "state_changed", "seq": seq})
    bridge._registry_snapshot = MagicMock(return_value=[])
    bridge._state_snapshot = MagicMock(return_value=[])
    bridge._queue_reconciliation_snapshots(15_261)
    frames = [bridge.queue.get_nowait() for _ in range(2)]
    assert [(f["type"], f["seq"]) for f in frames] == [("registry_snapshot", 15_261), ("state_snapshot", 15_262)]
    assert bridge.queue.empty()
    assert bridge._registry_snapshot.call_count == 1 and bridge._state_snapshot.call_count == 1
