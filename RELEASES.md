# Release and upgrade notes

## 1.0.0

- Config Flow pairing with Smart Villa URL and a 10-minute single-use code.
- Persistent outbound WSS with startup registry/state reconciliation.
- Entity registry updates, state changes, heartbeats, command acknowledgements, bounded queue, replay sequence, and exponential reconnect.
- Options Flow, reauthentication, unload/reload, TH/EN translations, repairs, and redacted diagnostics.

### Upgrade

This is the first HACS release. Existing Smart Villa REST token and HA webhook installations can remain active during the production soak. Pair the HACS integration before retiring the fallback paths.
