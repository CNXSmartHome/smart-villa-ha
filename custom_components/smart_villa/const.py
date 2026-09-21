"""Constants for the Smart Villa integration."""

DOMAIN = "smart_villa"
CONF_INSTALLATION_ID = "installation_id"
CONF_CREDENTIAL = "credential"
CONF_WEBSOCKET_URL = "websocket_url"
CONF_CREDENTIAL_VERSION = "credential_version"
SCHEMA_VERSION = 1
INTEGRATION_VERSION = "1.0.0"
SUPPORTED_DOMAINS = {"light", "switch", "climate", "cover", "fan", "input_boolean", "scene", "binary_sensor", "sensor"}
COMMAND_CAPABILITIES = {
    "SWITCH_ON",
    "SWITCH_OFF",
    "LIGHT_SET_BRIGHTNESS",
    "CLIMATE_TURN_ON",
    "CLIMATE_TURN_OFF",
    "CLIMATE_SET_TEMPERATURE",
    "CURTAIN_OPEN",
    "CURTAIN_CLOSE",
    "SCENE_ACTIVATE",
}
MAX_QUEUE_SIZE = 1000
HEARTBEAT_SECONDS = 15
MAX_BACKOFF_SECONDS = 300
