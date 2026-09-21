"""Pairing API client. Sensitive response values are never logged."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urljoin

from aiohttp import ClientError, ClientSession


class SmartVillaApiError(Exception):
    """Safe API failure with a non-secret category."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class PairingResult:
    installation_id: str
    credential: str
    credential_version: int
    websocket_url: str


async def exchange_pairing_code(
    session: ClientSession, base_url: str, pairing_code: str, integration_version: str, ha_version: str
) -> PairingResult:
    endpoint = urljoin(f"{base_url.rstrip('/')}/", "api/integrations/home-assistant/pair")
    try:
        async with session.post(
            endpoint,
            json={
                "pairing_code": pairing_code.strip().upper(),
                "integration_version": integration_version,
                "ha_version": ha_version,
            },
            timeout=15,
        ) as response:
            body = await response.json(content_type=None)
            if response.status == 429:
                raise SmartVillaApiError("rate_limited")
            if response.status in (401, 403):
                raise SmartVillaApiError("invalid_pairing_code")
            if response.status >= 400:
                raise SmartVillaApiError("cannot_connect")
    except SmartVillaApiError:
        raise
    except (ClientError, TimeoutError, ValueError) as err:
        raise SmartVillaApiError("cannot_connect") from err

    try:
        return PairingResult(
            installation_id=str(body["installation_id"]),
            credential=str(body["credential"]),
            credential_version=int(body["credential_version"]),
            websocket_url=str(body["websocket_url"]),
        )
    except (KeyError, TypeError, ValueError) as err:
        raise SmartVillaApiError("invalid_response") from err
