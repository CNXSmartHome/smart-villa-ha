"""Redacted diagnostics for Smart Villa."""

from homeassistant.components.diagnostics import async_redact_data

from .const import DOMAIN

TO_REDACT = {"credential", "pairing_code", "websocket_url", "url"}


async def async_get_config_entry_diagnostics(hass, entry):
    bridge = hass.data[DOMAIN][entry.entry_id]
    return {
        "entry": async_redact_data(dict(entry.data), TO_REDACT),
        "options": dict(entry.options),
        "status": async_redact_data(dict(bridge.status), TO_REDACT),
        "queue_depth": bridge.queue.qsize(),
        "sequence": bridge.sequence,
    }
