from __future__ import annotations

from typing import Any

import httpx

POLYGON_CHAIN_ID = "0x89"
CTF_CONTRACT_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
PAYOUT_DENOMINATOR_SELECTOR = "dd34de67"
PAYOUT_NUMERATORS_SELECTOR = "0504c814"


class EvmRpcError(RuntimeError):
    pass


def _normalize_hex32(value: str) -> str:
    raw = str(value).strip().lower()
    if raw.startswith("0x"):
        raw = raw[2:]
    if not raw:
        raise ValueError("condition_id is empty")
    if len(raw) > 64:
        raise ValueError(f"condition_id is longer than 32 bytes: {value!r}")
    try:
        int(raw, 16)
    except ValueError as exc:
        raise ValueError(f"condition_id is not hex: {value!r}") from exc
    return raw.rjust(64, "0")


def _uint256(value: int) -> str:
    if value < 0:
        raise ValueError("uint256 cannot be negative")
    return f"{value:064x}"


def _parse_uint256(result: Any) -> int:
    if not isinstance(result, str) or not result.startswith("0x"):
        raise EvmRpcError(f"eth_call returned non-hex result: {result!r}")
    if result in {"0x", "0x0"}:
        return 0
    return int(result, 16)


class PolygonCtfClient:
    def __init__(
        self,
        rpc_url: str,
        *,
        contract_address: str = CTF_CONTRACT_ADDRESS,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 20.0,
    ) -> None:
        self.rpc_url = rpc_url
        self.contract_address = contract_address
        self._client = httpx.AsyncClient(transport=transport, timeout=timeout_s)
        self._request_id = 0

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "PolygonCtfClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def _rpc(self, method: str, params: list[Any]) -> Any:
        self._request_id += 1
        response = await self._client.post(
            self.rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": self._request_id,
                "method": method,
                "params": params,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise EvmRpcError(f"unexpected RPC payload: {payload!r}")
        if payload.get("error") is not None:
            raise EvmRpcError(str(payload["error"]))
        return payload.get("result")

    async def chain_id(self) -> str:
        result = await self._rpc("eth_chainId", [])
        if not isinstance(result, str):
            raise EvmRpcError(f"unexpected chain id: {result!r}")
        return result.lower()

    async def ensure_polygon(self) -> None:
        chain_id = await self.chain_id()
        if chain_id != POLYGON_CHAIN_ID:
            raise EvmRpcError(f"expected Polygon chain {POLYGON_CHAIN_ID}, got {chain_id}")

    async def eth_call(self, data: str) -> str:
        result = await self._rpc(
            "eth_call",
            [{"to": self.contract_address, "data": data}, "latest"],
        )
        if not isinstance(result, str):
            raise EvmRpcError(f"eth_call returned non-string result: {result!r}")
        return result

    async def payout_denominator(self, condition_id: str) -> int:
        data = "0x" + PAYOUT_DENOMINATOR_SELECTOR + _normalize_hex32(condition_id)
        return _parse_uint256(await self.eth_call(data))

    async def payout_numerator(self, condition_id: str, outcome_index: int) -> int:
        data = (
            "0x"
            + PAYOUT_NUMERATORS_SELECTOR
            + _normalize_hex32(condition_id)
            + _uint256(outcome_index)
        )
        return _parse_uint256(await self.eth_call(data))

    async def payout_vector(self, condition_id: str, outcome_count: int) -> tuple[int, list[int]]:
        denominator = await self.payout_denominator(condition_id)
        numerators = [
            await self.payout_numerator(condition_id, outcome_index)
            for outcome_index in range(outcome_count)
        ]
        return denominator, numerators


__all__ = [
    "CTF_CONTRACT_ADDRESS",
    "EvmRpcError",
    "PAYOUT_DENOMINATOR_SELECTOR",
    "PAYOUT_NUMERATORS_SELECTOR",
    "POLYGON_CHAIN_ID",
    "PolygonCtfClient",
]
