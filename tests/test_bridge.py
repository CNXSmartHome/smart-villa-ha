from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from custom_components.smart_villa.bridge import SmartVillaBridge


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
