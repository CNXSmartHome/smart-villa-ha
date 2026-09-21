"""Config and options flows for Smart Villa."""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_URL
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import SmartVillaApiError, exchange_pairing_code
from .const import (
    CONF_CREDENTIAL,
    CONF_CREDENTIAL_VERSION,
    CONF_INSTALLATION_ID,
    CONF_WEBSOCKET_URL,
    DOMAIN,
    HEARTBEAT_SECONDS,
    INTEGRATION_VERSION,
)

PAIR_SCHEMA = vol.Schema({vol.Required(CONF_URL): str, vol.Required("pairing_code"): str})


class SmartVillaConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Pair using only the Smart Villa URL and a single-use code."""

    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None):
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                paired = await exchange_pairing_code(
                    async_get_clientsession(self.hass),
                    user_input[CONF_URL],
                    user_input["pairing_code"],
                    INTEGRATION_VERSION,
                    HA_VERSION,
                )
                await self.async_set_unique_id(paired.installation_id)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title="Smart Villa",
                    data={
                        CONF_URL: user_input[CONF_URL].rstrip("/"),
                        CONF_INSTALLATION_ID: paired.installation_id,
                        CONF_CREDENTIAL: paired.credential,
                        CONF_CREDENTIAL_VERSION: paired.credential_version,
                        CONF_WEBSOCKET_URL: paired.websocket_url,
                    },
                )
            except SmartVillaApiError as err:
                errors["base"] = err.code
        return self.async_show_form(step_id="user", data_schema=PAIR_SCHEMA, errors=errors)

    async def async_step_reauth(self, entry_data: dict[str, Any]):
        self._reauth_entry = self.hass.config_entries.async_get_entry(self.context["entry_id"])
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input: dict[str, Any] | None = None):
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                paired = await exchange_pairing_code(
                    async_get_clientsession(self.hass),
                    user_input[CONF_URL],
                    user_input["pairing_code"],
                    INTEGRATION_VERSION,
                    HA_VERSION,
                )
                if paired.installation_id != self._reauth_entry.unique_id:
                    return self.async_abort(reason="wrong_installation")
                return self.async_update_reload_and_abort(
                    self._reauth_entry,
                    data_updates={
                        CONF_URL: user_input[CONF_URL].rstrip("/"),
                        CONF_CREDENTIAL: paired.credential,
                        CONF_CREDENTIAL_VERSION: paired.credential_version,
                        CONF_WEBSOCKET_URL: paired.websocket_url,
                    },
                )
            except SmartVillaApiError as err:
                errors["base"] = err.code
        return self.async_show_form(step_id="reauth_confirm", data_schema=PAIR_SCHEMA, errors=errors)

    @staticmethod
    def async_get_options_flow(config_entry):
        return SmartVillaOptionsFlow()


class SmartVillaOptionsFlow(config_entries.OptionsFlow):
    async def async_step_init(self, user_input: dict[str, Any] | None = None):
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        "heartbeat_seconds",
                        default=self.config_entry.options.get("heartbeat_seconds", HEARTBEAT_SECONDS),
                    ): vol.All(int, vol.Range(min=10, max=60)),
                }
            ),
        )
