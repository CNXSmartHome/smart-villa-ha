# Release and upgrade notes

## 1.0.1

Intended release tag: `v1.0.1` (create only after this change is approved).

- Recover automatically when Smart Villa reports an out-of-order sequence.
- Supervise WebSocket sender, receiver, and heartbeat tasks so a transport error
  always enters bounded reconnect backoff.
- Clear stale queued events after reconnect and reconcile with fresh registry and
  state snapshots using the server-provided next sequence.

### Upgrade

No re-pair is required. Keep the REST token and webhook fallback enabled during
the rollout and verify a registry/state reconciliation after upgrading.

## 1.0.0

- Config Flow pairing with Smart Villa URL and a 10-minute single-use code.
- Persistent outbound WSS with startup registry/state reconciliation.
- Entity registry updates, state changes, heartbeats, command acknowledgements, bounded queue, replay sequence, and exponential reconnect.
- Options Flow, reauthentication, unload/reload, TH/EN translations, repairs, and redacted diagnostics.

### Upgrade

This is the first HACS release. Existing Smart Villa REST token and HA webhook installations can remain active during the production soak. Pair the HACS integration before retiring the fallback paths.
