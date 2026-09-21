"""Repair flow for connection failures."""

from homeassistant.helpers import issue_registry as ir


class CannotConnectRepairFlow(ir.RepairsFlow):
    async def async_step_init(self, user_input=None):
        return self.async_create_entry(title="", data={})


async def async_create_fix_flow(hass, issue_id, data):
    return CannotConnectRepairFlow()
