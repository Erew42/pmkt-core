from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from pmkt.resolution.evm import EvmRpcError, PolygonCtfClient
from pmkt.runtime import OperationExpiry


BLOCK_HASH = "0x" + "ab" * 32
TRANSACTION_HASH = "0x" + "cd" * 32


@pytest.mark.asyncio
async def test_polygon_ctf_native_block_and_receipt_reads_preserve_fields() -> None:
    header = {
        "number": "0x123",
        "hash": BLOCK_HASH,
        "parentHash": "0x" + "ef" * 32,
        "timestamp": "0x6553f100",
        "transactions": [TRANSACTION_HASH],
    }
    receipt = {
        "transactionHash": TRANSACTION_HASH,
        "blockHash": BLOCK_HASH,
        "blockNumber": "0x123",
        "status": "0x1",
        "gasUsed": "0x5208",
        "effectiveGasPrice": hex(2**200 + 1),
        "logs": [{"data": "0x" + "ff" * 32, "logIndex": "0x0"}],
    }
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        requests.append(payload)
        result = {
            "eth_blockNumber": "0x123",
            "eth_getBlockByNumber": header,
            "eth_getTransactionReceipt": receipt,
        }[payload["method"]]
        return httpx.Response(200, json={"result": result})

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        height = await client.block_number()
        assert height == 291
        assert type(height) is int
        assert await client.block_by_number(height) == header
        upper_hash = "0x" + TRANSACTION_HASH[2:].upper()
        assert await client.transaction_receipt(upper_hash) == receipt

    assert [(item["method"], item["params"]) for item in requests] == [
        ("eth_blockNumber", []),
        ("eth_getBlockByNumber", ["0x123", False]),
        ("eth_getTransactionReceipt", [TRANSACTION_HASH]),
    ]
    assert [item["id"] for item in requests] == [1, 2, 3]


@pytest.mark.parametrize(
    ("block", "expected"),
    [
        (0, "0x0"),
        (291, "0x123"),
        ("0xABC", "0xabc"),
        ("earliest", "earliest"),
        ("latest", "latest"),
        ("safe", "safe"),
        ("finalized", "finalized"),
        ("pending", "pending"),
        ({"blockNumber": "0x123"}, {"blockNumber": "0x123"}),
        ({"blockNumber": 291}, {"blockNumber": "0x123"}),
        ({"blockHash": BLOCK_HASH}, {"blockHash": BLOCK_HASH}),
        (
            {"blockHash": BLOCK_HASH, "requireCanonical": False},
            {"blockHash": BLOCK_HASH, "requireCanonical": False},
        ),
        (
            {"blockHash": BLOCK_HASH, "requireCanonical": True},
            {"blockHash": BLOCK_HASH, "requireCanonical": True},
        ),
    ],
)
@pytest.mark.asyncio
async def test_polygon_ctf_payout_vector_forwards_one_block(
    block: Any, expected: Any
) -> None:
    requests = []
    values = [2**200 + 3, 2**200 + 1, 2]

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        requests.append(payload)
        return httpx.Response(200, json={"result": hex(values[(len(requests) - 1) % 3])})

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        for condition in ("0xabc", "0xdef"):
            denominator, numerators = await client.payout_vector(condition, 2, block=block)
            assert (denominator, numerators) == (values[0], values[1:])
            assert all(type(value) is int for value in [denominator, *numerators])

    assert len(requests) == 6
    assert all(item["params"][1] == expected for item in requests)
    assert requests[0]["params"][0]["data"] == "0xdd34de67" + "abc".rjust(64, "0")
    assert requests[2]["params"][0]["data"].endswith("1".rjust(64, "0"))


@pytest.mark.asyncio
async def test_polygon_ctf_call_and_payout_defaults_remain_latest() -> None:
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"result": "0x1"})

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        assert await client.eth_call("0x1234") == "0x1"
        assert await client.payout_denominator("0xabc") == 1
        assert await client.payout_numerator("0xabc", 0) == 1
        assert await client.payout_vector("0xabc", 2) == (1, [1, 1])

    assert len(requests) == 6
    assert all(item["params"][1] == "latest" for item in requests)


@pytest.mark.asyncio
async def test_polygon_ctf_reuses_selected_header_hash_and_snapshots_selector() -> None:
    requests = []
    selector: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        requests.append(payload)
        if payload["method"] == "eth_getBlockByNumber":
            result: Any = {"number": "0x123", "hash": BLOCK_HASH, "timestamp": "0x42"}
        else:
            selector["blockHash"] = "0x" + "00" * 32
            result = "0x1"
        return httpx.Response(200, json={"result": result})

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        header = await client.block_by_number("finalized")
        assert header is not None
        selector.update(blockHash=header["hash"], requireCanonical=True)
        assert await client.payout_vector("0xabc", 2, block=selector) == (1, [1, 1])

    assert requests[0]["params"] == ["finalized", False]
    assert all(
        item["params"][1] == {"blockHash": BLOCK_HASH, "requireCanonical": True}
        for item in requests[1:]
    )


@pytest.mark.parametrize(
    "block",
    [
        True, False, -1, 1.5, None, "", "123", "0x", "0x00", "0x01", "0x-1",
        "0x+1", "0x1_0", "0xgg", " latest ", "LATEST", {}, [],
        {"blockNumber": True}, {"blockNumber": "latest"},
        {"blockHash": "0x123"}, {"blockHash": True},
        {"blockHash": BLOCK_HASH, "blockNumber": "0x1"},
        {"blockHash": BLOCK_HASH, "requireCanonical": 1},
        {"blockNumber": "0x1", "requireCanonical": True},
        {"blockHash": BLOCK_HASH, "extra": False},
    ],
)
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_invalid_call_blocks_before_io(block: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid block reached transport")

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ValueError):
            await client.eth_call("0x1234", block=block)
        with pytest.raises(ValueError):
            await client.payout_denominator("0xabc", block=block)
        with pytest.raises(ValueError):
            await client.payout_numerator("0xabc", 0, block=block)
        with pytest.raises(ValueError):
            await client.payout_vector("0xabc", 2, block=block)


@pytest.mark.parametrize("block", [True, -1, 1.5, None, "0x01", "0xgg", {}])
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_invalid_header_blocks_before_io(block: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid header block reached transport")

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ValueError):
            await client.block_by_number(block)


@pytest.mark.parametrize("value", [True, 123, None, "", "0x1", "0x" + "ab" * 31, "0x" + "gg" * 32])
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_invalid_receipt_hash_before_io(value: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid transaction hash reached transport")

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ValueError):
            await client.transaction_receipt(value)


@pytest.mark.parametrize("condition", [True, 123, None, "", "-1", "+1", "a_b", "0xgg", "a" * 65])
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_invalid_condition_before_io(condition: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid condition reached transport")

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ValueError):
            await client.payout_vector(condition, 2)


@pytest.mark.parametrize("value", [True, False, -1, 1.5, "1", None, 2**256])
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_invalid_outcome_index_before_io(value: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid outcome index reached transport")

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ValueError):
            await client.payout_numerator("0xabc", value)


@pytest.mark.parametrize("value", [True, False, -1, 0, 1.5, "2", None])
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_invalid_outcome_count_before_any_io(value: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid outcome count reached transport")

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ValueError):
            await client.payout_vector("0xabc", value)


@pytest.mark.parametrize("value", [True, None, "0x1", "0xzz", "0x" + "ab" * 21])
def test_polygon_ctf_rejects_invalid_contract_address(value: Any) -> None:
    with pytest.raises(ValueError, match="contract_address"):
        PolygonCtfClient("https://rpc.test", contract_address=value)


@pytest.mark.parametrize("value", [True, None, "1234", "0x1", "0xzz"])
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_invalid_call_data_before_io(value: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid call data reached transport")

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ValueError, match="data"):
            await client.eth_call(value)


@pytest.mark.asyncio
async def test_polygon_ctf_rejects_rpc_writes_before_io() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("write RPC reached transport")

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ValueError, match="read-only"):
            await client._rpc("eth_sendRawTransaction", ["0x1234"])
        with pytest.raises(ValueError, match="read-only"):
            await client._rpc("eth_sendTransaction", [{}])


@pytest.mark.asyncio
async def test_polygon_ctf_native_null_is_distinct_from_missing_rpc_result() -> None:
    payloads = iter([{"result": None}, {"result": None}, {}])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=next(payloads))

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        assert await client.block_by_number(291) is None
        assert await client.transaction_receipt(TRANSACTION_HASH) is None
        with pytest.raises(EvmRpcError, match="no result"):
            await client.transaction_receipt(TRANSACTION_HASH)


@pytest.mark.parametrize("result", [[], "0x1", True, 1])
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_nonobject_block_and_receipt_payloads(result: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": result})

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(EvmRpcError, match="block metadata"):
            await client.block_by_number(291)
        with pytest.raises(EvmRpcError, match="transaction receipt"):
            await client.transaction_receipt(TRANSACTION_HASH)


@pytest.mark.parametrize("result", [None, True, 291, "0x", "0x01", "0x-1", "0xgg"])
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_malformed_block_number(result: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": result})

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(EvmRpcError, match="block number"):
            await client.block_number()


@pytest.mark.parametrize("result", ["0x", "0x-1", "0x+1", "0x1_0", "0xgg", "0x1" + "0" * 64])
@pytest.mark.asyncio
async def test_polygon_ctf_rejects_malformed_uint256_payout(result: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": result})

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(EvmRpcError, match="malformed uint256"):
            await client.payout_denominator("0xabc", block=291)


@pytest.mark.asyncio
async def test_polygon_ctf_zero_and_maximum_payouts_remain_exact_integers() -> None:
    results = iter(["0x0", "0x" + "ff" * 32])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"result": next(results)})

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        assert await client.payout_denominator("0xabc", block=291) == 0
        assert await client.payout_numerator("0xabc", 2**256 - 1, block=291) == 2**256 - 1


@pytest.mark.parametrize("error", ["missing trie node", "block not found", "block is not canonical"])
@pytest.mark.asyncio
async def test_polygon_ctf_historical_failure_never_falls_back_to_latest(error: str) -> None:
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"error": {"code": -32000, "message": error}})

    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(EvmRpcError, match=error):
            await client.payout_vector("0xabc", 2, block={"blockHash": BLOCK_HASH})

    assert len(requests) == 1
    assert requests[0]["params"][1] == {"blockHash": BLOCK_HASH}


@pytest.mark.asyncio
async def test_polygon_ctf_new_reads_and_payouts_share_operation_expiry() -> None:
    timeouts = []

    def handler(request: httpx.Request) -> httpx.Response:
        timeouts.append(request.extensions["timeout"])
        method = json.loads(request.content)["method"]
        result: Any = "0x1" if method in {"eth_call", "eth_blockNumber"} else None
        return httpx.Response(200, json={"result": result})

    expiry = OperationExpiry.after(2, clock=lambda: 100)
    async with PolygonCtfClient(
        "https://rpc.test", transport=httpx.MockTransport(handler), timeout_s=20
    ) as client:
        await client.block_number(expiry=expiry)
        await client.block_by_number(291, expiry=expiry)
        await client.transaction_receipt(TRANSACTION_HASH, expiry=expiry)
        await client.payout_vector("0xabc", 2, block=291, expiry=expiry)

    assert len(timeouts) == 6
    assert all(value == 2 for timeout in timeouts for value in timeout.values())
