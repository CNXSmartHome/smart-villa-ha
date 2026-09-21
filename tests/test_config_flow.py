from unittest.mock import AsyncMock, patch

from homeassistant import config_entries
from homeassistant.const import CONF_URL
from homeassistant.data_entry_flow import FlowResultType

from custom_components.smart_villa.api import PairingResult, SmartVillaApiError
from custom_components.smart_villa.const import DOMAIN


async def test_config_flow_pairs_with_url_and_code_only(hass):
    paired = PairingResult("install-1", "redacted-credential", 1, "wss://villa.example/ws")
    with patch("custom_components.smart_villa.config_flow.exchange_pairing_code", AsyncMock(return_value=paired)):
        result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": config_entries.SOURCE_USER})
        assert result["type"] is FlowResultType.FORM
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {CONF_URL: "https://villa.example", "pairing_code": "ABCDEFGHJK"}
        )
        assert result["type"] is FlowResultType.CREATE_ENTRY
        assert result["data"]["installation_id"] == "install-1"


async def test_config_flow_redacts_pairing_failure(hass):
    with patch(
        "custom_components.smart_villa.config_flow.exchange_pairing_code",
        AsyncMock(side_effect=SmartVillaApiError("invalid_pairing_code")),
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": config_entries.SOURCE_USER},
            data={CONF_URL: "https://villa.example", "pairing_code": "SECRET-CODE"},
        )
        assert result["errors"] == {"base": "invalid_pairing_code"}
