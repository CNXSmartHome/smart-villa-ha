"""Repair flow for connection failures."""

from __future__ import annotations

from typing import Any

from homeassistant.components.repairs import RepairsFlow
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResult


class CannotConnectRepairFlow(RepairsFlow):
    """Acknowledge-only flow: the bridge reconnects by itself; the issue clears on the next successful auth."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        return self.async_create_entry(title="", data={})


async def async_create_fix_flow(hass: HomeAssistant, issue_id: str, data: dict[str, Any] | None) -> RepairsFlow:
    return CannotConnectRepairFlow()
