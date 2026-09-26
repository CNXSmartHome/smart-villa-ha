"""Constants for the Smart Villa integration."""

DOMAIN = "smart_villa"
CONF_INSTALLATION_ID = "installation_id"
CONF_CREDENTIAL = "credential"
CONF_WEBSOCKET_URL = "websocket_url"
CONF_CREDENTIAL_VERSION = "credential_version"
SCHEMA_VERSION = 1
INTEGRATION_VERSION = "1.0.3"
SUPPORTED_DOMAINS = {"light", "switch", "climate", "cover", "fan", "input_boolean", "scene", "binary_sensor", "sensor"}
COMMAND_CAPABILITIES = {
    "SWITCH_ON",
    "SWITCH_OFF",
    "LIGHT_SET_BRIGHTNESS",
    "CLIMATE_TURN_ON",
    "CLIMATE_TURN_OFF",
    "CLIMATE_SET_TEMPERATURE",
    "CLIMATE_SET_FAN_MODE",
    "CURTAIN_OPEN",
    "CURTAIN_CLOSE",
    "SCENE_ACTIVATE",
}
MAX_QUEUE_SIZE = 1000
HEARTBEAT_SECONDS = 15
# Reconnect schedule (seconds) for the 1st, 2nd, 3rd, 4th consecutive failed attempt: 1, 2, 5, 15; then 30.
# Jitter (0–1 s) is added before the 30 s cap is applied.
# 1.0.1 doubled up to 300 s and never reset, so after a handful of drops every reconnect waited ~5 minutes.
RECONNECT_BACKOFF_STEPS = (1.0, 2.0, 5.0, 15.0)
MAX_BACKOFF_SECONDS = 30
# Reconnect when the server has been silent (no ack / command / error) for this many heartbeat intervals.
HEARTBEAT_MISS_LIMIT = 2
