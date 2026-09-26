# Release and upgrade notes

## 1.0.3

Intended release tag: `v1.0.3` (tag only after the Smart Villa PR with #386 is merged and deployed).

- New command capability `CLIMATE_SET_FAN_MODE` → `climate.set_fan_mode` (`params.fanMode`). Smart Villa offers it to
  guests only for AC entities that report `fan_modes`, and only when this integration reports version ≥ 1.0.3, so
  older installs never receive it. Rejected with `INVALID_PARAMS` for non-climate entities or a non-string / empty /
  > 24-char mode. Door/lock domains remain refused (`TTLOCK_ONLY` on the server; not in `SUPPORTED_DOMAINS` here).
- No protocol/schema change (`SCHEMA_VERSION` stays 1); state events already carry all attributes, so `fan_mode` /
  `fan_modes` reach Smart Villa without changes.

### Upgrade

HACS → Smart Villa → update to 1.0.3 → restart Home Assistant. No re-pair. Check Smart Villa `HaInstallation`
`integrationVersion = 1.0.3`; the fan speed row appears on `/g/room` for ACs whose HA entity lists `fan_modes`.


## 1.0.2

Intended release tag: `v1.0.2` (create only after this change is approved; do not tag from this PR).

Incident 2026-09-23 (Smart Villa `INCIDENT-GUEST-QR-2026-09-23.md`): after every bridge drop the integration waited
~300 s before reconnecting, leaving Smart Villa blind for 5 minutes per deploy/network blip.

- Root cause: the reconnect loop only reset its backoff when `_connected_session()` returned normally — which never
  happens (a session always ends by raising). Each drop therefore doubled the delay for the lifetime of the
  integration: 1, 2, 4 … 256, 300, 300, … Production had accumulated the cap.
- Backoff is now 1, 2, 5, 15 s for the 1st–4th consecutive failed attempt, then 30 s; jitter (0–1 s) is added
  before the 30 s cap. It resets whenever the previous session had authenticated — only repeated failures to
  *establish* a session escalate.
- Reconnect triggers: socket close/error, any of sender / receiver / heartbeat task ending or raising, and a new
  heartbeat-acknowledgement watchdog — no server frame for 2 heartbeat intervals (30 s) ends the session
  (`HeartbeatLost`), so a half-open socket behind a recreated proxy is detected without waiting for TCP.
- After reconnect: unchanged — `auth_ok.next_sequence` is adopted, the stale outbound queue is cleared, one
  registry + one state snapshot are sent.
- `command_ack` is sent on every success/failure path; a full outbound queue drops the oldest queued event to make
  room for an ack instead of dropping the ack, and the reconciliation close is deferred to the sender so the ack
  is transmitted before the socket closes. Task cancellation is never masked as a failure ack.
- Diagnostics gain `consecutive_failures`.
- Fix `repairs` platform import (`homeassistant.helpers.issue_registry` has no `RepairsFlow`; it lives in
  `homeassistant.components.repairs`) — HA 2026.9 logged an import error on every load.

### Upgrade

No re-pair required. After upgrading, restart the Smart Villa Caddy/bridge (or cut the network for 20 s) and confirm
the integration reconnects within ~1–5 s and Smart Villa shows the installation CONNECTED within 45 s.

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
