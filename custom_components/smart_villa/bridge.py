"""Persistent outbound authenticated WSS bridge to Smart Villa OS."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from aiohttp import ClientWebSocketResponse, WSMsgType
from homeassistant.const import EVENT_STATE_CHANGED
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import Event, HomeAssistant
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.issue_registry import IssueSeverity, async_create_issue, async_delete_issue
from homeassistant.helpers.storage import Store

from .const import (
    COMMAND_CAPABILITIES,
    CONF_CREDENTIAL,
    CONF_CREDENTIAL_VERSION,
    CONF_INSTALLATION_ID,
    CONF_WEBSOCKET_URL,
    DOMAIN,
    HEARTBEAT_SECONDS,
    INTEGRATION_VERSION,
    MAX_BACKOFF_SECONDS,
    MAX_QUEUE_SIZE,
    SCHEMA_VERSION,
    SUPPORTED_DOMAINS,
)

_LOGGER = logging.getLogger(__name__)


class ReconciliationRequired(ConnectionError):
    """The server requires a clean reconnect and fresh snapshots."""


class SmartVillaBridge:
    def __init__(self, hass: HomeAssistant, entry) -> None:
        self.hass = hass
        self.entry = entry
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(MAX_QUEUE_SIZE)
        self.store = Store(hass, 1, f"{DOMAIN}.{entry.entry_id}")
        self.sequence = 0
        self.task: asyncio.Task | None = None
        self._websocket: ClientWebSocketResponse | None = None
        self._stopping = False
        self._unsubscribers: list[Any] = []
        self.status: dict[str, Any] = {
            "connected": False,
            "last_error": None,
            "last_heartbeat": None,
            "last_state_event": None,
            "reconnects": 0,
        }

    async def async_start(self) -> None:
        saved = await self.store.async_load() or {}
        self.sequence = int(saved.get("sequence", 0))
        self._unsubscribers.append(self.hass.bus.async_listen(EVENT_STATE_CHANGED, self._state_changed))
        self._unsubscribers.append(self.hass.bus.async_listen(er.EVENT_ENTITY_REGISTRY_UPDATED, self._registry_changed))
        self.task = self.hass.async_create_background_task(self._run(), f"{DOMAIN}-bridge", eager_start=True)

    async def async_stop(self) -> None:
        self._stopping = True
        for unsubscribe in self._unsubscribers:
            unsubscribe()
        self._unsubscribers.clear()
        if self._websocket is not None:
            await self._websocket.close(code=1001, message=b"integration unload")
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
        await self.store.async_save({"sequence": self.sequence})

    def _clear_outbound_queue(self) -> None:
        while not self.queue.empty():
            with suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
                self.queue.task_done()

    def _queue_reconciliation_snapshots(self, next_sequence: int) -> None:
        self.sequence = next_sequence - 1
        self._clear_outbound_queue()
        self._enqueue(self._message("registry_snapshot", entities=self._registry_snapshot()))
        self._enqueue(self._message("state_snapshot", states=self._state_snapshot()))

    def _message(self, kind: str, **payload: Any) -> dict[str, Any]:
        self.sequence += 1
        return {
            "v": SCHEMA_VERSION,
            "id": str(uuid4()),
            "type": kind,
            "ts": datetime.now(UTC).isoformat(),
            "seq": self.sequence,
            **payload,
        }

    def _enqueue(self, message: dict[str, Any]) -> None:
        if self.queue.full():
            self.status["last_error"] = "outbound_queue_overflow"
            if self._websocket is not None:
                self.hass.async_create_task(self._websocket.close(code=1013, message=b"reconcile"))
            return
        self.queue.put_nowait(message)

    async def _state_changed(self, event: Event) -> None:
        new_state = event.data.get("new_state")
        if new_state is None or new_state.domain not in SUPPORTED_DOMAINS:
            return
        self.status["last_state_event"] = datetime.now(UTC).isoformat()
        self._enqueue(self._message("state_changed", event=self._serialize_state(new_state)))

    async def _registry_changed(self, _event: Event) -> None:
        self._enqueue(self._message("registry_snapshot", entities=self._registry_snapshot()))

    def _serialize_state(self, state) -> dict[str, Any]:
        return {
            "entity_id": state.entity_id,
            "state": state.state,
            "attributes": dict(state.attributes),
            "last_changed": state.last_changed.isoformat(),
            "last_updated": state.last_updated.isoformat(),
        }

    def _registry_snapshot(self) -> list[dict[str, Any]]:
        entities = er.async_get(self.hass)
        devices = dr.async_get(self.hass)
        areas = ar.async_get(self.hass)
        result = []
        for entity in entities.entities.values():
            domain = entity.entity_id.split(".", 1)[0]
            if domain not in SUPPORTED_DOMAINS:
                continue
            device = devices.async_get(entity.device_id) if entity.device_id else None
            area_id = entity.area_id or (device.area_id if device else None)
            area = areas.async_get_area(area_id) if area_id else None
            result.append(
                {
                    "entity_id": entity.entity_id,
                    "unique_id": entity.unique_id,
                    "platform": entity.platform,
                    "name": entity.name,
                    "original_name": entity.original_name,
                    "device_id": entity.device_id,
                    "area_id": area_id,
                    "area_name": area.name if area else None,
                    "disabled_by": str(entity.disabled_by) if entity.disabled_by else None,
                    "hidden_by": str(entity.hidden_by) if entity.hidden_by else None,
                }
            )
        return result

    def _state_snapshot(self) -> list[dict[str, Any]]:
        return [
            self._serialize_state(state) for state in self.hass.states.async_all() if state.domain in SUPPORTED_DOMAINS
        ]

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stopping:
            try:
                await self._connected_session()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 - category only, never secret values
                self.status.update(
                    {"connected": False, "last_error": type(err).__name__, "reconnects": self.status["reconnects"] + 1}
                )
                _LOGGER.warning("Smart Villa bridge disconnected (%s); retrying", type(err).__name__)
                async_create_issue(
                    self.hass,
                    DOMAIN,
                    "connection",
                    is_fixable=False,
                    severity=IssueSeverity.WARNING,
                    translation_key="connection_failed",
                )
                await asyncio.sleep(backoff + random.uniform(0, min(1.0, backoff / 4)))
                backoff = min(MAX_BACKOFF_SECONDS, backoff * 2)

    async def _connected_session(self) -> None:
        session = async_get_clientsession(self.hass)
        async with session.ws_connect(
            self.entry.data[CONF_WEBSOCKET_URL], heartbeat=30, max_msg_size=256 * 1024
        ) as websocket:
            self._websocket = websocket
            await websocket.send_json(
                {
                    "v": SCHEMA_VERSION,
                    "id": str(uuid4()),
                    "type": "auth",
                    "ts": datetime.now(UTC).isoformat(),
                    "installation_id": self.entry.data[CONF_INSTALLATION_ID],
                    "credential": self.entry.data[CONF_CREDENTIAL],
                    "integration_version": INTEGRATION_VERSION,
                    "ha_version": HA_VERSION,
                }
            )
            auth = await asyncio.wait_for(websocket.receive_json(), timeout=10)
            if auth.get("type") != "auth_ok":
                if auth.get("code") == "AUTH_INVALID":
                    self.entry.async_start_reauth(self.hass)
                raise PermissionError("authentication rejected")
            next_sequence = int(auth.get("next_sequence", self.sequence + 1))
            self.status.update({"connected": True, "last_error": None})
            async_delete_issue(self.hass, DOMAIN, "connection")
            self._queue_reconciliation_snapshots(next_sequence)
            await self.store.async_save({"sequence": self.sequence})
            sender = asyncio.create_task(self._sender(websocket))
            heartbeat = asyncio.create_task(self._heartbeat())
            receiver = asyncio.create_task(self._receiver(websocket))
            tasks = {sender, heartbeat, receiver}
            try:
                done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    exception = task.exception()
                    if exception is not None:
                        raise exception
                raise ConnectionError("bridge session ended")
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._websocket = None
                self.status["connected"] = False

    async def _receiver(self, websocket: ClientWebSocketResponse) -> None:
        async for message in websocket:
            if message.type == WSMsgType.TEXT:
                await self._handle_server_message(websocket, message.json())
            elif message.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                return

    async def _sender(self, websocket: ClientWebSocketResponse) -> None:
        while True:
            message = await self.queue.get()
            try:
                await websocket.send_json(message)
                await self.store.async_save({"sequence": self.sequence})
            finally:
                self.queue.task_done()

    async def _heartbeat(self) -> None:
        interval = self.entry.options.get("heartbeat_seconds", HEARTBEAT_SECONDS)
        while True:
            await asyncio.sleep(interval)
            self.status["last_heartbeat"] = datetime.now(UTC).isoformat()
            self._enqueue(self._message("heartbeat"))

    async def _handle_server_message(self, websocket: ClientWebSocketResponse, message: dict[str, Any]) -> None:
        kind = message.get("type")
        if kind == "command":
            await self._handle_command(websocket, message)
        elif kind == "error" and (
            message.get("code") == "OUT_OF_ORDER" or message.get("reconciliation_required") is True
        ):
            expected = message.get("expected_sequence")
            received = message.get("received_sequence")
            self.status.update(
                {
                    "last_error": "sequence_reconciliation",
                    "sequence_error": {
                        "category": "OUT_OF_ORDER",
                        "expected_sequence": expected if isinstance(expected, int) else None,
                        "received_sequence": received if isinstance(received, int) else None,
                    },
                }
            )
            _LOGGER.warning("Smart Villa bridge requested sequence reconciliation (OUT_OF_ORDER)")
            await websocket.close(code=4009, message=b"reconciliation required")
            raise ReconciliationRequired("OUT_OF_ORDER")
        elif kind == "credential_rotate":
            credential = message.get("credential")
            version = message.get("credential_version")
            if isinstance(credential, str) and isinstance(version, int):
                self.hass.config_entries.async_update_entry(
                    self.entry, data={**self.entry.data, CONF_CREDENTIAL: credential, CONF_CREDENTIAL_VERSION: version}
                )
                self._enqueue(
                    self._message("error_ack", ref_id=message.get("id", "unknown"), error_code="CREDENTIAL_STORED")
                )

    async def _handle_command(self, websocket: ClientWebSocketResponse, message: dict[str, Any]) -> None:
        command_id = str(message.get("command_id", ""))
        capability = str(message.get("capability", ""))
        entity_id = str(message.get("entity_id", ""))
        try:
            if capability not in COMMAND_CAPABILITIES:
                raise ValueError("CAPABILITY_DENIED")
            domain = entity_id.split(".", 1)[0]
            if domain in {"lock", "alarm_control_panel", "button"}:
                raise ValueError("TTLOCK_ONLY")
            expires_at = datetime.fromisoformat(str(message["expires_at"]).replace("Z", "+00:00"))
            if expires_at.timestamp() <= time.time():
                raise ValueError("COMMAND_EXPIRED")
            service, data = self._service_call(capability, entity_id, message.get("params") or {})
            await self.hass.services.async_call(
                domain if capability not in {"SCENE_ACTIVATE"} else "scene", service, data, blocking=True
            )
            self._enqueue(self._message("command_ack", command_id=command_id, success=True))
        except Exception as err:  # noqa: BLE001
            code = str(err) if isinstance(err, ValueError) else "SERVICE_CALL_FAILED"
            self._enqueue(self._message("command_ack", command_id=command_id, success=False, error_code=code))

    @staticmethod
    def _service_call(capability: str, entity_id: str, params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        data: dict[str, Any] = {"entity_id": entity_id}
        mapping = {
            "SWITCH_ON": "turn_on",
            "SWITCH_OFF": "turn_off",
            "CLIMATE_TURN_ON": "turn_on",
            "CLIMATE_TURN_OFF": "turn_off",
            "CURTAIN_OPEN": "open_cover",
            "CURTAIN_CLOSE": "close_cover",
            "SCENE_ACTIVATE": "turn_on",
        }
        if capability == "LIGHT_SET_BRIGHTNESS":
            data["brightness_pct"] = int(params.get("brightness", 60))
            return "turn_on", data
        if capability == "CLIMATE_SET_TEMPERATURE":
            data["temperature"] = float(params.get("temperature", params.get("temp", 25)))
            return "set_temperature", data
        return mapping[capability], data
