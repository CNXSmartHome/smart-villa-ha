"""Smart Villa Home Assistant integration."""

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .bridge import SmartVillaBridge
from .const import DOMAIN


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    bridge = SmartVillaBridge(hass, entry)
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = bridge
    await bridge.async_start()
    entry.async_on_unload(entry.add_update_listener(_reload_entry))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    bridge = hass.data[DOMAIN].pop(entry.entry_id)
    await bridge.async_stop()
    return True


async def _reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)
