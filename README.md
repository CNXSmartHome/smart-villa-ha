# Smart Villa for Home Assistant

Commercial outbound integration between Home Assistant and Smart Villa OS. Home Assistant opens one authenticated WSS connection and sends registry metadata, initial state, state changes, heartbeats, and command acknowledgements. Smart Villa OS sends policy-approved device commands on that connection.

## Install with HACS

1. Add `https://github.com/CNXSmartHome/smart-villa-ha` as a custom integration repository in HACS.
2. Install **Smart Villa** and restart Home Assistant.
3. In Smart Villa OS, open **Manage → Smart Home → HA Quick Setup** and generate a pairing code.
4. In Home Assistant, add the **Smart Villa** integration and enter the Smart Villa URL and pairing code.

The pairing code is single use and expires after 10 minutes. The integration does not require YAML, an inbound Home Assistant port, a webhook secret, or a Home Assistant Long-Lived Access Token.

## Security

- Each installation is scoped to one Smart Villa property.
- Commands are checked by Smart Villa OS before transmission and checked again by this integration.
- Lock and access commands are rejected. TTLock remains the only guest door authority.
- Diagnostics redact URLs, credentials, pairing codes, and WSS endpoints.
- Credentials can be rotated or revoked from Smart Villa OS.

## Connectivity

The integration reconnects with exponential backoff and jitter after Home Assistant, Smart Villa OS, or network restarts. Every connection performs registry and state reconciliation. Outbound messages use persistent sequence numbers; duplicate and out-of-order messages receive explicit acknowledgements from Smart Villa OS.

## Development

```bash
python -m pip install -r requirements_test.txt
pytest
```

Pull requests run `pytest-homeassistant-custom-component`, hassfest, and HACS validation.
