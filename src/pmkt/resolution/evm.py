from __future__ import annotations

import re
from typing import Any

import httpx

from pmkt.runtime import OperationExpiry

POLYGON_CHAIN_ID = "0x89"
CTF_CONTRACT_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
PAYOUT_DENOMINATOR_SELECTOR = "dd34de67"
PAYOUT_NUMERATORS_SELECTOR = "0504c814"
_BLOCK_TAGS = {"earliest", "latest", "pending", "safe", "finalized"}
_READ_METHODS = {
    "eth_chainId",
    "eth_blockNumber",
    "eth_getBlockByNumber",
    "eth_getTransactionReceipt",
    "eth_call",
}


class EvmRpcError(RuntimeError):
    pass


def _normalize_hex32(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("condition_id must be a hex string")
    raw = value.strip().lower()
    if raw.startswith("0x"):
        raw = raw[2:]
    if not raw:
        raise ValueError("condition_id is empty")
    if len(raw) > 64:
        raise ValueError(f"condition_id is longer than 32 bytes: {value!r}")
    if re.fullmatch(r"[0-9a-f]+", raw) is None:
        raise ValueError(f"condition_id is not hex: {value!r}")
    return raw.rjust(64, "0")


def _uint256(value: int) -> str:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 2**256:
        raise ValueError("uint256 must be an integer from 0 through 2**256 - 1")
    return f"{value:064x}"


def _hex_data(value: str, name: str, *, size_bytes: int | None = None) -> str:
    if not isinstance(value, str) or re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", value) is None:
        raise ValueError(f"{name} must be a 0x-prefixed, whole-byte hex string")
    if size_bytes is not None and len(value) != 2 + 2 * size_bytes:
        raise ValueError(f"{name} must contain exactly {size_bytes} bytes")
    return value.lower()


def _block_number_parameter(block: int | str) -> str:
    if isinstance(block, int) and not isinstance(block, bool) and block >= 0:
        return hex(block)
    if isinstance(block, str):
        if block in _BLOCK_TAGS:
            return block
        if re.fullmatch(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)", block) is not None:
            return block.lower()
    raise ValueError(
        "block must be a nonnegative integer, canonical hex quantity, or block tag"
    )


def _call_block_parameter(block: int | str | dict[str, Any]) -> str | dict[str, Any]:
    if not isinstance(block, dict):
        return _block_number_parameter(block)
    if set(block) == {"blockNumber"}:
        number = _block_number_parameter(block["blockNumber"])
        if number in _BLOCK_TAGS:
            raise ValueError("blockNumber must be a block number, not a tag")
        return {"blockNumber": number}
    if "blockHash" in block and set(block) <= {"blockHash", "requireCanonical"}:
        result: dict[str, Any] = {
            "blockHash": _hex_data(block["blockHash"], "blockHash", size_bytes=32)
        }
        if "requireCanonical" in block:
            if not isinstance(block["requireCanonical"], bool):
                raise ValueError("requireCanonical must be a boolean")
            result["requireCanonical"] = block["requireCanonical"]
        return result
    raise ValueError("block object must identify exactly one blockNumber or blockHash")


def _parse_uint256(result: Any) -> int:
    if not isinstance(result, str) or not result.startswith("0x"):
        raise EvmRpcError(f"eth_call returned non-hex result: {result!r}")
    if re.fullmatch(r"0x[0-9a-fA-F]{1,64}", result) is None:
        raise EvmRpcError(f"eth_call returned malformed uint256: {result!r}")
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
        self.contract_address = _hex_data(
            contract_address, "contract_address", size_bytes=20
        )
        self.timeout_s = timeout_s
        self._client = httpx.AsyncClient(transport=transport, timeout=timeout_s)
        self._request_id = 0

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "PolygonCtfClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def _rpc(
        self,
        method: str,
        params: list[Any],
        *,
        expiry: OperationExpiry | None = None,
    ) -> Any:
        if method not in _READ_METHODS:
            raise ValueError(f"unsupported read-only RPC method: {method!r}")
        self._request_id += 1
        request_id = self._request_id

        async def send() -> httpx.Response:
            timeout = (
                expiry.capped_timeout(self.timeout_s) if expiry is not None else None
            )
            kwargs: dict[str, Any] = {
                "json": {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                }
            }
            if timeout is not None:
                kwargs["timeout"] = timeout
            return await self._client.post(self.rpc_url, **kwargs)

        response = await expiry.run(send) if expiry is not None else await send()
        try:
            response.raise_for_status()
            if expiry is not None:
                expiry.checkpoint()
            payload = response.json()
            if expiry is not None:
                expiry.checkpoint()
        finally:
            await response.aclose()
        if not isinstance(payload, dict):
            raise EvmRpcError(f"unexpected RPC payload: {payload!r}")
        if payload.get("error") is not None:
            raise EvmRpcError(str(payload["error"]))
        if "result" not in payload:
            raise EvmRpcError("RPC payload has no result")
        return payload["result"]

    async def chain_id(self, *, expiry: OperationExpiry | None = None) -> str:
        result = await self._rpc("eth_chainId", [], expiry=expiry)
        if not isinstance(result, str):
            raise EvmRpcError(f"unexpected chain id: {result!r}")
        return result.lower()

    async def ensure_polygon(self, *, expiry: OperationExpiry | None = None) -> None:
        chain_id = await self.chain_id(expiry=expiry)
        if chain_id != POLYGON_CHAIN_ID:
            raise EvmRpcError(
                f"expected Polygon chain {POLYGON_CHAIN_ID}, got {chain_id}"
            )

    async def block_number(self, *, expiry: OperationExpiry | None = None) -> int:
        """Return the node's current block height as an exact integer."""
        result = await self._rpc("eth_blockNumber", [], expiry=expiry)
        if not isinstance(result, str) or re.fullmatch(
            r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)", result
        ) is None:
            raise EvmRpcError(f"unexpected block number: {result!r}")
        return int(result, 16)

    async def block_by_number(
        self, block: int | str = "latest", *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any] | None:
        """Return native block metadata with transaction hashes, or None if unknown."""
        number = _block_number_parameter(block)
        result = await self._rpc("eth_getBlockByNumber", [number, False], expiry=expiry)
        if result is not None and not isinstance(result, dict):
            raise EvmRpcError(f"unexpected block metadata: {result!r}")
        return result

    async def transaction_receipt(
        self, transaction_hash: str, *, expiry: OperationExpiry | None = None
    ) -> dict[str, Any] | None:
        """Return a native receipt, or None when the node has no mined receipt."""
        transaction_hash = _hex_data(transaction_hash, "transaction_hash", size_bytes=32)
        result = await self._rpc(
            "eth_getTransactionReceipt", [transaction_hash], expiry=expiry
        )
        if result is not None and not isinstance(result, dict):
            raise EvmRpcError(f"unexpected transaction receipt: {result!r}")
        return result

    async def eth_call(
        self,
        data: str,
        *,
        block: int | str | dict[str, Any] = "latest",
        expiry: OperationExpiry | None = None,
    ) -> str:
        data = _hex_data(data, "data")
        address = _hex_data(self.contract_address, "contract_address", size_bytes=20)
        block_parameter = _call_block_parameter(block)
        result = await self._rpc(
            "eth_call",
            [{"to": address, "data": data}, block_parameter],
            expiry=expiry,
        )
        if not isinstance(result, str):
            raise EvmRpcError(f"eth_call returned non-string result: {result!r}")
        return result

    async def payout_denominator(
        self,
        condition_id: str,
        *,
        block: int | str | dict[str, Any] = "latest",
        expiry: OperationExpiry | None = None,
    ) -> int:
        data = "0x" + PAYOUT_DENOMINATOR_SELECTOR + _normalize_hex32(condition_id)
        return _parse_uint256(await self.eth_call(data, block=block, expiry=expiry))

    async def payout_numerator(
        self,
        condition_id: str,
        outcome_index: int,
        *,
        block: int | str | dict[str, Any] = "latest",
        expiry: OperationExpiry | None = None,
    ) -> int:
        data = (
            "0x"
            + PAYOUT_NUMERATORS_SELECTOR
            + _normalize_hex32(condition_id)
            + _uint256(outcome_index)
        )
        return _parse_uint256(await self.eth_call(data, block=block, expiry=expiry))

    async def payout_vector(
        self,
        condition_id: str,
        outcome_count: int,
        *,
        block: int | str | dict[str, Any] = "latest",
        expiry: OperationExpiry | None = None,
    ) -> tuple[int, list[int]]:
        if (
            isinstance(outcome_count, bool)
            or not isinstance(outcome_count, int)
            or outcome_count <= 0
        ):
            raise ValueError("outcome_count must be a positive integer")
        block_parameter = _call_block_parameter(block)
        denominator = await self.payout_denominator(
            condition_id, block=block_parameter, expiry=expiry
        )
        numerators = [
            await self.payout_numerator(
                condition_id, outcome_index, block=block_parameter, expiry=expiry
            )
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
