from __future__ import annotations

import copy
import json
from typing import Any

import httpx
import pytest

from pmkt.resolution.evm import EvmRpcError
from pmkt.resolution.evm_logs import (
    ORDER_FILLED_TOPIC,
    POLYMARKET_EXCHANGE_CONTRACTS,
    SIDE_BUY,
    SIDE_SELL,
    OrderFilled,
    PolygonLogReader,
    decode_order_filled,
    split_block_range,
)

EXCHANGE = "0xe2222d279d744050d28e00520010520000310f59"
OTHER = "0xe111180000d2663c0091e4f400237545b87b996b"
TOKEN_ID = 31342321808194667617057401536286865100444776460174249578741015594347489327945

TOKEN_WORD = "454b2020471e02167e56bf86421347b29bd8b799243c2dfe967e5740694efb49"


def word(value: int) -> str:
    return format(value, "064x")


# Two real logs of Polygon block 96701606 (0x5c38ca6), as returned by a public node.
MAKER_FILL_LOG: dict[str, Any] = {
    "address": EXCHANGE,
    "topics": [
        ORDER_FILLED_TOPIC,
        "0x18b621e1a98d17e5d6d51ffb883c46867b16523e3cf7bb36f31bfd0e36e6d3ca",
        "0x000000000000000000000000c602e34792d9a5e129f3404daef2df43d1d37fc1",
        "0x000000000000000000000000ad5353afe30c2da57709e2704ef3ccdcf67eef24",
    ],
    "data": "0x"
    + word(1)
    + TOKEN_WORD
    + word(20_000_000)
    + word(2_380_000)
    + word(0)
    + word(0)
    + word(0),
    "blockNumber": "0x5c38ca6",
    "transactionHash": "0xd7537cfa2d79f63687bcc943c72d5b14474359ea767b5eb32a13ca3f71ca5dbe",
    "transactionIndex": "0x11",
    "blockHash": "0xeb2bd5815c8b6aa5848043eb6583600431d4a4ad9d10b6648d35eb30190a7b5b",
    "blockTimestamp": "0x6aea92a9",
    "logIndex": "0xae",
    "removed": False,
}
TAKER_ORDER_LOG: dict[str, Any] = {
    "address": EXCHANGE,
    "topics": [
        ORDER_FILLED_TOPIC,
        "0x89910b0899e3f7e0e6da3e49a8781da22ad513cf71268e3523deec9b881449d6",
        "0x000000000000000000000000ad5353afe30c2da57709e2704ef3ccdcf67eef24",
        "0x000000000000000000000000e2222d279d744050d28e00520010520000310f59",
    ],
    "data": "0x"
    + word(0)
    + TOKEN_WORD
    + word(12_880_000)
    + word(95_000_000)
    + word(445_340)
    + "ec6bee0b946246d8a243f327de02038ddcd120f6df034a2abfd66586f24b43c4"
    + word(0),
    "blockNumber": "0x5c38ca6",
    "transactionHash": "0xd7537cfa2d79f63687bcc943c72d5b14474359ea767b5eb32a13ca3f71ca5dbe",
    "transactionIndex": "0x11",
    "blockHash": "0xeb2bd5815c8b6aa5848043eb6583600431d4a4ad9d10b6648d35eb30190a7b5b",
    "blockTimestamp": "0x6aea92a9",
    "logIndex": "0xb3",
    "removed": False,
}
BLOCK = 96701606


def log_at(block: int, index: int, **changes: Any) -> dict[str, Any]:
    log = copy.deepcopy(MAKER_FILL_LOG)
    log["blockNumber"] = hex(block)
    log["logIndex"] = hex(index)
    log["blockHash"] = "0x" + format(block, "064x")
    log.update(changes)
    return log


def reader(handler: Any) -> PolygonLogReader:
    return PolygonLogReader("https://rpc.test", transport=httpx.MockTransport(handler))


def answer(request: httpx.Request, **fields: Any) -> httpx.Response:
    """A JSON-RPC reply that echoes the id of the request."""
    payload = json.loads(request.content)
    return httpx.Response(200, json={"jsonrpc": "2.0", "id": payload["id"], **fields})


def replying(result: Any, requests: list[dict[str, Any]] | None = None) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        if requests is not None:
            requests.append(json.loads(request.content))
        return answer(request, result=result)

    return handler


def test_get_logs_request_shape_and_native_result() -> None:
    requests: list[dict[str, Any]] = []
    logs = [log_at(100, 0), log_at(100, 1), log_at(105, 0)]
    with reader(replying(logs, requests)) as chain:
        result = chain.get_logs(
            100,
            109,
            address=["0x" + EXCHANGE[2:].upper().replace("X", "x"), OTHER],
            topics=[ORDER_FILLED_TOPIC.upper().replace("X", "x"), None, [MAKER_FILL_LOG["topics"][2]]],
        )

    assert result == logs  # the node's dictionaries, unchanged
    assert requests == [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [
                {
                    "fromBlock": "0x64",
                    "toBlock": "0x6d",
                    "address": [EXCHANGE, OTHER],
                    "topics": [ORDER_FILLED_TOPIC, None, [MAKER_FILL_LOG["topics"][2]]],
                }
            ],
        }
    ]


def test_get_logs_without_filters_sends_only_the_range_and_accepts_empty_result() -> None:
    requests: list[dict[str, Any]] = []
    with reader(replying([], requests)) as chain:
        assert chain.get_logs(0, 0) == []
        assert chain.get_logs(5, 5, address=EXCHANGE) == []
    assert requests[0]["params"] == [{"fromBlock": "0x0", "toBlock": "0x0"}]
    assert requests[1]["params"] == [{"fromBlock": "0x5", "toBlock": "0x5", "address": EXCHANGE}]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"from_block": True, "to_block": 5},
        {"from_block": -1, "to_block": 5},
        {"from_block": 1.5, "to_block": 5},
        {"from_block": "0x1", "to_block": 5},
        {"from_block": 1, "to_block": "latest"},
        {"from_block": 6, "to_block": 5},
        {"from_block": 1, "to_block": 5, "address": "0x1234"},
        {"from_block": 1, "to_block": 5, "address": []},
        {"from_block": 1, "to_block": 5, "address": 5},
        {"from_block": 1, "to_block": 5, "topics": [ORDER_FILLED_TOPIC] * 5},
        {"from_block": 1, "to_block": 5, "topics": ORDER_FILLED_TOPIC},
        {"from_block": 1, "to_block": 5, "topics": ["0x1234"]},
        {"from_block": 1, "to_block": 5, "topics": [[]]},
        {"from_block": 1, "to_block": 5, "topics": [5]},
    ],
)
def test_invalid_arguments_fail_before_io(kwargs: dict[str, Any]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid arguments reached transport")

    with reader(handler) as chain:
        with pytest.raises(ValueError):
            chain.get_logs(**kwargs)
        with pytest.raises(ValueError):
            chain.iter_logs(**kwargs, blocks_per_piece=2)  # raises at the call, not at next()


def test_rpc_allowlist_rejects_write_methods_before_io() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("write RPC reached transport")

    with reader(handler) as chain:
        for method in ("eth_sendRawTransaction", "eth_sendTransaction", "eth_call", "eth_blockNumber"):
            with pytest.raises(ValueError, match="read-only"):
                chain._rpc(method, [])


def test_ensure_polygon() -> None:
    with reader(replying("0x89")) as chain:
        chain.ensure_polygon()
    with reader(replying("0x1")) as chain:
        with pytest.raises(EvmRpcError, match="Polygon"):
            chain.ensure_polygon()


@pytest.mark.parametrize(
    ("logs", "message"),
    [
        ([log_at(99, 0)], "outside the requested range"),
        ([log_at(110, 0)], "outside the requested range"),
        ([log_at(100, 0, removed=True)], "removed"),
        ([log_at(100, 0, removed="false")], "removed"),
        ([log_at(100, 1), log_at(100, 0)], "out-of-order"),
        ([log_at(101, 0), log_at(100, 5)], "out-of-order"),
        ([log_at(100, 1), log_at(100, 1)], "duplicate"),
        ([log_at(100, 0), log_at(100, 1, blockHash="0x" + "11" * 32)], "different block hashes"),
        ([log_at(100, 0, address=OTHER)], "was not requested"),
        ([log_at(100, 0, topics=[ORDER_FILLED_TOPIC])], "do not match"),
        ([log_at(100, 0, topics=["0x" + "22" * 32] + MAKER_FILL_LOG["topics"][1:])], "do not match"),
        ([log_at(100, 0, blockNumber=None)], "malformed"),
        ([log_at(100, 0, blockNumber="0x064")], "malformed"),
        ([log_at(100, 0, logIndex=3)], "malformed"),
        ([log_at(100, 0, transactionHash="0x12")], "malformed"),
        ([log_at(100, 0, data="0x1")], "malformed"),
        ([log_at(100, 0, blockHash=None)], "malformed"),
        ([log_at(100, 0, topics="nope")], "malformed"),
        ([log_at(100, 0, blockTimestamp="soon")], "malformed"),
        ([log_at(100, 0, topics=MAKER_FILL_LOG["topics"] + ["0x" + "00" * 32])], "at most 4 topics"),
        (["not a log"], "non-object"),
        ({"logs": []}, "non-list"),
        (None, "non-list"),
    ],
)
def test_get_logs_rejects_bad_responses(logs: Any, message: str) -> None:
    with reader(replying(logs)) as chain:
        with pytest.raises(EvmRpcError, match=message):
            chain.get_logs(
                100, 109, address=EXCHANGE, topics=[ORDER_FILLED_TOPIC, None, MAKER_FILL_LOG["topics"][2]]
            )


def test_wildcard_topic_position_still_requires_a_topic_there() -> None:
    short = log_at(100, 0, topics=[ORDER_FILLED_TOPIC])
    with reader(replying([short])) as chain:
        with pytest.raises(EvmRpcError, match="do not match"):
            chain.get_logs(100, 109, topics=[ORDER_FILLED_TOPIC, None])
    with reader(replying([short])) as chain:
        assert chain.get_logs(100, 109, topics=[ORDER_FILLED_TOPIC]) == [short]


@pytest.mark.parametrize("reply", [{"id": 9999, "result": []}, {"id": None, "result": []}, {"result": []}])
def test_response_must_echo_the_request_id(reply: dict[str, Any]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"jsonrpc": "2.0", **reply})

    with reader(handler) as chain:
        with pytest.raises(EvmRpcError, match="does not match request id"):
            chain.get_logs(1, 2)


def test_get_logs_accepts_missing_removed_and_missing_block_timestamp() -> None:
    log = log_at(100, 0)
    del log["removed"]
    del log["blockTimestamp"]
    with reader(replying([log])) as chain:
        assert chain.get_logs(100, 100) == [log]


def test_range_boundaries_are_inclusive() -> None:
    logs = [log_at(100, 0), log_at(109, 0)]
    with reader(replying(logs)) as chain:
        assert chain.get_logs(100, 109) == logs


def test_get_logs_reports_node_errors_and_http_failures_without_retry() -> None:
    requests: list[int] = []

    def refuse(request: httpx.Request) -> httpx.Response:
        requests.append(1)
        return answer(request, error={"code": 35, "message": "ranges over 10000 blocks"})

    with reader(refuse) as chain:
        with pytest.raises(EvmRpcError, match="35"):
            chain.get_logs(1, 200)

    def overloaded(request: httpx.Request) -> httpx.Response:
        requests.append(1)
        return httpx.Response(503)

    with reader(overloaded) as chain:
        with pytest.raises(httpx.HTTPStatusError):
            chain.get_logs(1, 2)

    def no_result(request: httpx.Request) -> httpx.Response:
        requests.append(1)
        return answer(request)

    with reader(no_result) as chain:
        with pytest.raises(EvmRpcError, match="no result"):
            chain.get_logs(1, 2)

    assert len(requests) == 3  # one request each: nothing was retried


@pytest.mark.parametrize(
    ("first", "last", "size", "expected"),
    [
        (100, 109, 5, [(100, 104), (105, 109)]),
        (100, 111, 5, [(100, 104), (105, 109), (110, 111)]),  # short last piece
        (100, 110, 5, [(100, 104), (105, 109), (110, 110)]),  # last piece of one block
        (100, 102, 100, [(100, 102)]),  # range smaller than a piece
        (100, 199, 100, [(100, 199)]),  # exactly one full piece
        (100, 200, 100, [(100, 199), (200, 200)]),
        (7, 7, 1, [(7, 7)]),
        (0, 2, 1, [(0, 0), (1, 1), (2, 2)]),
    ],
)
def test_split_block_range(first: int, last: int, size: int, expected: list[tuple[int, int]]) -> None:
    pieces = split_block_range(first, last, size)
    assert pieces == expected
    covered = [block for a, b in pieces for block in range(a, b + 1)]
    assert covered == list(range(first, last + 1))  # no gap, no overlap


@pytest.mark.parametrize("size", [0, -1, True, 2.0, None, "5"])
def test_split_block_range_rejects_bad_piece_size(size: Any) -> None:
    with pytest.raises(ValueError, match="blocks_per_piece"):
        split_block_range(1, 10, size)


def test_iter_logs_requests_pieces_lazily_with_bounds() -> None:
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        requests.append(payload)
        query = payload["params"][0]
        first = int(query["fromBlock"], 16)
        return answer(request, result=[log_at(first, 0)])

    with reader(handler) as chain:
        pieces = chain.iter_logs(100, 111, blocks_per_piece=5, address=EXCHANGE)
        assert requests == []  # nothing is requested until the caller iterates
        first_piece = next(pieces)
        assert (first_piece.first_block, first_piece.last_block) == (100, 104)
        assert first_piece.logs == [log_at(100, 0)]
        assert len(requests) == 1
        rest = list(pieces)

    assert [(p.first_block, p.last_block) for p in rest] == [(105, 109), (110, 111)]
    assert [(r["params"][0]["fromBlock"], r["params"][0]["toBlock"]) for r in requests] == [
        ("0x64", "0x68"),
        ("0x69", "0x6d"),
        ("0x6e", "0x6f"),
    ]
    assert all(r["params"][0]["address"] == EXCHANGE for r in requests)


def test_iter_logs_failing_piece_raises_and_later_pieces_are_not_requested() -> None:
    requests: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = json.loads(request.content)["params"][0]
        first = int(query["fromBlock"], 16)
        requests.append(first)
        if first == 105:
            return answer(request, error={"code": -32000, "message": "boom"})
        return answer(request, result=[])

    with reader(handler) as chain:
        saved: list[tuple[int, int]] = []
        with pytest.raises(EvmRpcError, match="boom"):
            for piece in chain.iter_logs(100, 119, blocks_per_piece=5):
                saved.append((piece.first_block, piece.last_block))

    assert saved == [(100, 104)]
    assert requests == [100, 105]  # 110 and 115 were never requested, 105 never retried


def test_iter_logs_does_not_materialize_piece_boundaries() -> None:
    requests: list[dict[str, Any]] = []
    with reader(replying([], requests)) as chain:
        # 10**15 pieces: a list of bounds would exhaust memory, a lazy iterator does not.
        pieces = chain.iter_logs(0, 10**15, blocks_per_piece=1)
        first = next(pieces)
    assert (first.first_block, first.last_block) == (0, 0)
    assert len(requests) == 1


def test_iter_logs_resume_keeps_piece_boundaries() -> None:
    boundaries = split_block_range(100, 123, 10)
    resumed = split_block_range(boundaries[1][0], 123, 10)
    assert resumed == boundaries[1:]


def test_decode_both_real_logs_with_exact_integers() -> None:
    maker_fill = decode_order_filled(MAKER_FILL_LOG)
    taker_order = decode_order_filled(TAKER_ORDER_LOG)

    zero = "0x" + "00" * 32
    assert maker_fill == OrderFilled(
        block_number=96701606,
        log_index=174,
        transaction_hash="0xd7537cfa2d79f63687bcc943c72d5b14474359ea767b5eb32a13ca3f71ca5dbe",
        transaction_index=17,
        contract=EXCHANGE,
        order_hash="0x18b621e1a98d17e5d6d51ffb883c46867b16523e3cf7bb36f31bfd0e36e6d3ca",
        maker="0xc602e34792d9a5e129f3404daef2df43d1d37fc1",
        taker="0xad5353afe30c2da57709e2704ef3ccdcf67eef24",
        side=SIDE_SELL,
        token_id=TOKEN_ID,
        maker_amount_filled=20_000_000,
        taker_amount_filled=2_380_000,
        fee=0,
        builder=zero,
        metadata=zero,
        block_timestamp=0x6AEA92A9,
    )
    assert taker_order == OrderFilled(
        block_number=96701606,
        log_index=179,
        transaction_hash="0xd7537cfa2d79f63687bcc943c72d5b14474359ea767b5eb32a13ca3f71ca5dbe",
        transaction_index=17,
        contract=EXCHANGE,
        order_hash="0x89910b0899e3f7e0e6da3e49a8781da22ad513cf71268e3523deec9b881449d6",
        maker="0xad5353afe30c2da57709e2704ef3ccdcf67eef24",
        taker=EXCHANGE,  # a taker-order summary: the decoder reports it, it does not classify
        side=SIDE_BUY,
        token_id=TOKEN_ID,
        maker_amount_filled=12_880_000,
        taker_amount_filled=95_000_000,
        fee=445_340,
        builder="0xec6bee0b946246d8a243f327de02038ddcd120f6df034a2abfd66586f24b43c4",
        metadata=zero,
        block_timestamp=1793757865,
    )
    ints = [
        taker_order.block_number, taker_order.log_index, taker_order.transaction_index,
        taker_order.side, taker_order.token_id, taker_order.maker_amount_filled,
        taker_order.taker_amount_filled, taker_order.fee, taker_order.block_timestamp,
    ]  # fmt: skip
    assert all(type(value) is int for value in ints)


def test_decode_keeps_uint256_extremes_exact_and_lowercases_addresses() -> None:
    log = copy.deepcopy(TAKER_ORDER_LOG)
    log["address"] = EXCHANGE.upper().replace("X", "x")
    log["topics"][2] = "0x" + "0" * 24 + "AD5353AFE30C2DA57709E2704EF3CCDCF67EEF24"
    max_word = "ff" * 32
    log["data"] = "0x" + "0" * 63 + "1" + max_word * 4 + "00" * 64
    decoded = decode_order_filled(log)
    assert decoded.contract == EXCHANGE
    assert decoded.maker == "0xad5353afe30c2da57709e2704ef3ccdcf67eef24"
    assert decoded.side == SIDE_SELL
    assert decoded.token_id == decoded.maker_amount_filled == decoded.fee == 2**256 - 1


def test_decode_without_block_timestamp_gives_none() -> None:
    log = copy.deepcopy(TAKER_ORDER_LOG)
    del log["blockTimestamp"]
    assert decode_order_filled(log).block_timestamp is None


@pytest.mark.parametrize(
    "change",
    [
        {"address": OTHER.replace("e1111", "e9999")},  # not a Polymarket exchange
        {"topics": ["0x" + "11" * 32] + TAKER_ORDER_LOG["topics"][1:]},  # other event
        {"topics": TAKER_ORDER_LOG["topics"][:3]},  # V1-style indexed layout
        {"topics": TAKER_ORDER_LOG["topics"] + ["0x" + "00" * 32]},
        {"topics": [TAKER_ORDER_LOG["topics"][0], TAKER_ORDER_LOG["topics"][1], "0x" + "11" * 32, TAKER_ORDER_LOG["topics"][3]]},  # padded address
        {"data": TAKER_ORDER_LOG["data"][:-64]},  # six words
        {"data": TAKER_ORDER_LOG["data"] + "00" * 32},  # eight words
        {"data": "0x" + "0" * 62 + "02" + TAKER_ORDER_LOG["data"][66:]},  # side 2
        {"data": "0x"},
        {"blockNumber": "0x05c38ca6"},
        {"logIndex": 179},
    ],
)
def test_decode_rejects_anything_that_is_not_a_v2_order_filled(change: dict[str, Any]) -> None:
    log = {**TAKER_ORDER_LOG, **change}
    with pytest.raises(ValueError):
        decode_order_filled(log)


def test_decode_contract_set_can_be_overridden_explicitly() -> None:
    log = {**TAKER_ORDER_LOG, "address": "0x" + "12" * 20}
    with pytest.raises(ValueError, match="known Polymarket"):
        decode_order_filled(log)
    assert decode_order_filled(log, contracts=frozenset({"0x" + "12" * 20})).contract == "0x" + "12" * 20


def test_known_contracts_are_the_three_listed_exchanges() -> None:
    assert POLYMARKET_EXCHANGE_CONTRACTS == {
        "0xe111180000d2663c0091e4f400237545b87b996b",
        "0xe2222d279d744050d28e00520010520000310f59",
        "0xe3333700ca9d93003f00f0f71f8515005f6c00aa",
    }
