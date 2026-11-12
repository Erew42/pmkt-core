"""Read-only ``eth_getLogs`` over block ranges, and a Polymarket ``OrderFilled`` decoder.

The reader is synchronous on purpose: a research script can call it in a plain
``for`` loop. It shares the hex/block validators of :mod:`pmkt.resolution.evm`
but not its client, because that client is async and tied to the CTF contract.

Usage::

    with PolygonLogReader("https://polygon.drpc.org") as chain:
        for piece in chain.iter_logs(
            96_701_600, 96_701_699, blocks_per_piece=100, topics=[ORDER_FILLED_TOPIC]
        ):
            save(piece.logs)  # native node dictionaries
            fills = [decode_order_filled(log) for log in piece.logs]
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterator, NamedTuple, Sequence

import httpx

from pmkt.resolution.evm import EvmRpcError, POLYGON_CHAIN_ID, _hex_data

Log = dict[str, Any]

# Only these methods can leave this module; a write method fails before any I/O.
_READ_METHODS = {"eth_chainId", "eth_getLogs"}
_QUANTITY = re.compile(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)")
_MAX_TOPIC_POSITIONS = 4

# Polymarket CTF Exchange V2, https://github.com/Polymarket/ctf-exchange-v2 at
# ccc0596074f4dfd62c944fbca4de252893b82b4b, src/exchange/interfaces/ITrading.sol:
#   event OrderFilled(bytes32 indexed orderHash, address indexed maker,
#       address indexed taker, Side side, uint256 tokenId,
#       uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee,
#       bytes32 builder, bytes32 metadata);
# keccak256 of the canonical signature
# "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
# equals this topic 0 (checked against the source and against live logs).
ORDER_FILLED_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
# Contract addresses as listed on https://docs.polymarket.com/resources/contracts
# (CTF Exchange, Neg Risk CTF Exchange, Poly v2 exchange), lower-case.
POLYMARKET_EXCHANGE_CONTRACTS = frozenset(
    {
        "0xe111180000d2663c0091e4f400237545b87b996b",
        "0xe2222d279d744050d28e00520010520000310f59",
        "0xe3333700ca9d93003f00f0f71f8515005f6c00aa",
    }
)
SIDE_BUY = 0  # the contract's ``Side`` enum
SIDE_SELL = 1
_ORDER_FILLED_DATA_WORDS = 7


class LogPiece(NamedTuple):
    """Logs of one inclusive block range ``first_block`` .. ``last_block``."""

    first_block: int
    last_block: int
    logs: list[Log]


@dataclass(frozen=True)
class OrderFilled:
    """One decoded Polymarket V2 ``OrderFilled`` log. Amounts are raw integers.

    ``maker`` and ``taker`` are the log's own fields. In the summary log of a
    taker order ``taker`` is the emitting exchange contract; this decoder does
    not classify that. ``side`` is ``SIDE_BUY`` or ``SIDE_SELL`` as seen by
    ``maker``. ``maker_amount_filled`` is what the maker's order gave up and
    ``taker_amount_filled`` what it received (USDC and shares both have 6
    decimals, so base units are millionths). ``fee`` is the raw uint256 as
    emitted. ``builder`` and ``metadata`` are the order's signed bytes32
    fields, kept as 0x-prefixed hex. ``block_timestamp`` is the node's
    non-standard ``blockTimestamp`` (seconds) and is None when it is absent.
    """

    block_number: int
    log_index: int
    transaction_hash: str
    transaction_index: int
    contract: str
    order_hash: str
    maker: str
    taker: str
    side: int
    token_id: int
    maker_amount_filled: int
    taker_amount_filled: int
    fee: int
    builder: str
    metadata: str
    block_timestamp: int | None


def split_block_range(
    from_block: int, to_block: int, blocks_per_piece: int
) -> list[tuple[int, int]]:
    """Split ``from_block`` .. ``to_block`` (both inclusive) into fixed-size pieces.

    Every piece has ``blocks_per_piece`` blocks except possibly the last, which
    ends exactly at ``to_block``.
    """
    _check_range(from_block, to_block)
    if (
        isinstance(blocks_per_piece, bool)
        or not isinstance(blocks_per_piece, int)
        or blocks_per_piece < 1
    ):
        raise ValueError("blocks_per_piece must be a positive integer")
    return [
        (first, min(first + blocks_per_piece - 1, to_block))
        for first in range(from_block, to_block + 1, blocks_per_piece)
    ]


def _check_range(from_block: int, to_block: int) -> None:
    for name, value in (("from_block", from_block), ("to_block", to_block)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer block number")
    if from_block > to_block:
        raise ValueError("from_block must not be greater than to_block")


def _address_filter(address: str | Sequence[str] | None) -> str | list[str] | None:
    if address is None:
        return None
    if isinstance(address, str):
        return _hex_data(address, "address", size_bytes=20)
    if not isinstance(address, (list, tuple)):
        raise ValueError("address must be one address or a list of addresses")
    addresses = [_hex_data(item, "address", size_bytes=20) for item in address]
    if not addresses:
        raise ValueError("address list must not be empty")
    return addresses


def _topic_filter(
    topics: Sequence[str | Sequence[str] | None] | None,
) -> list[str | list[str] | None] | None:
    if topics is None:
        return None
    if not isinstance(topics, (list, tuple)) or len(topics) > _MAX_TOPIC_POSITIONS:
        raise ValueError(f"topics must be a sequence of at most {_MAX_TOPIC_POSITIONS} positions")
    result: list[str | list[str] | None] = []
    for position in topics:
        if position is None:
            result.append(None)
        elif isinstance(position, str):
            result.append(_hex_data(position, "topic", size_bytes=32))
        else:
            if not isinstance(position, (list, tuple)):
                raise ValueError("a topic position must be None, a topic, or a list of topics")
            options = [_hex_data(item, "topic", size_bytes=32) for item in position]
            if not options:
                raise ValueError("a topic position must not be an empty list")
            result.append(options)
    return result


def _field(log: Log, name: str) -> Any:
    return log.get(name)  # a missing field is None, which the validators reject


def _quantity(value: Any, name: str) -> int:
    if not isinstance(value, str) or _QUANTITY.fullmatch(value) is None:
        raise ValueError(f"{name} must be a canonical hex quantity: {value!r}")
    return int(value, 16)


def _topic_matches(
    log_topics: list[str], topic_filter: list[str | list[str] | None]
) -> bool:
    for index, wanted in enumerate(topic_filter):
        if wanted is None:
            continue
        if index >= len(log_topics):
            return False
        if isinstance(wanted, str):
            if log_topics[index] != wanted:
                return False
        elif log_topics[index] not in wanted:
            return False
    return True


def _validate_logs(
    result: Any,
    from_block: int,
    to_block: int,
    address_filter: str | list[str] | None,
    topic_filter: list[str | list[str] | None] | None,
) -> list[Log]:
    """Raise EvmRpcError unless ``result`` is a plausible, ordered log list."""
    if not isinstance(result, list):
        raise EvmRpcError(f"eth_getLogs returned a non-list result: {type(result).__name__}")
    allowed_addresses = (
        None
        if address_filter is None
        else {address_filter} if isinstance(address_filter, str) else set(address_filter)
    )
    previous: tuple[int, int] | None = None
    block_hashes: dict[int, str] = {}
    for log in result:
        if not isinstance(log, dict):
            raise EvmRpcError(f"eth_getLogs returned a non-object log: {log!r}")
        # ``removed`` is absent on some nodes; only an explicit true is a reorg signal.
        if log.get("removed", False) is not False:
            raise EvmRpcError(f"eth_getLogs returned a removed log: {log.get('removed')!r}")
        try:
            block = _quantity(log.get("blockNumber"), "blockNumber")
            index = _quantity(log.get("logIndex"), "logIndex")
            _quantity(log.get("transactionIndex"), "transactionIndex")
            if "blockTimestamp" in log:
                _quantity(log["blockTimestamp"], "blockTimestamp")
            _hex_data(_field(log, "transactionHash"), "transactionHash", size_bytes=32)
            _hex_data(_field(log, "data"), "data")
            address = _hex_data(_field(log, "address"), "address", size_bytes=20)
            block_hash = _hex_data(_field(log, "blockHash"), "blockHash", size_bytes=32)
            raw_topics = log.get("topics")
            if not isinstance(raw_topics, list):
                raise ValueError(f"topics must be a list: {raw_topics!r}")
            topics = [_hex_data(topic, "topic", size_bytes=32) for topic in raw_topics]
        except ValueError as exc:
            raise EvmRpcError(f"eth_getLogs returned a malformed log: {exc}") from exc

        if not from_block <= block <= to_block:
            raise EvmRpcError(
                f"log in block {block} is outside the requested range {from_block}..{to_block}"
            )
        if allowed_addresses is not None and address not in allowed_addresses:
            raise EvmRpcError(f"log address {address} was not requested")
        if topic_filter is not None and not _topic_matches(topics, topic_filter):
            raise EvmRpcError(f"log topics {topics} do not match the requested topics")
        if block_hashes.setdefault(block, block_hash) != block_hash:
            raise EvmRpcError(f"logs of block {block} carry different block hashes")
        position = (block, index)
        if previous is not None and position <= previous:
            kind = "duplicate" if position == previous else "out-of-order"
            raise EvmRpcError(
                f"{kind} log: (block, logIndex) {position} follows {previous}"
            )
        previous = position
    return result


class PolygonLogReader:
    """Synchronous, read-only ``eth_getLogs`` client. It never retries.

    A request either returns a validated list or raises: ``ValueError`` for bad
    arguments (before any I/O), ``EvmRpcError`` for node errors and for a
    response that fails validation, and ``httpx`` errors for HTTP failures.
    """

    def __init__(
        self,
        rpc_url: str,
        *,
        transport: httpx.BaseTransport | None = None,
        timeout_s: float = 60.0,
    ) -> None:
        self.rpc_url = rpc_url
        self.timeout_s = timeout_s
        self._client = httpx.Client(transport=transport, timeout=timeout_s)
        self._request_id = 0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> PolygonLogReader:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _rpc(self, method: str, params: list[Any]) -> Any:
        if method not in _READ_METHODS:
            raise ValueError(f"unsupported read-only RPC method: {method!r}")
        self._request_id += 1
        response = self._client.post(
            self.rpc_url,
            json={"jsonrpc": "2.0", "id": self._request_id, "method": method, "params": params},
        )
        try:
            response.raise_for_status()
            payload = response.json()
        finally:
            response.close()
        if not isinstance(payload, dict):
            raise EvmRpcError(f"unexpected RPC payload: {payload!r}")
        if payload.get("error") is not None:
            raise EvmRpcError(str(payload["error"]))
        if "result" not in payload:
            raise EvmRpcError("RPC payload has no result")
        return payload["result"]

    def ensure_polygon(self) -> None:
        chain_id = self._rpc("eth_chainId", [])
        if not isinstance(chain_id, str) or chain_id.lower() != POLYGON_CHAIN_ID:
            raise EvmRpcError(f"expected Polygon chain {POLYGON_CHAIN_ID}, got {chain_id!r}")

    def get_logs(
        self,
        from_block: int,
        to_block: int,
        *,
        address: str | Sequence[str] | None = None,
        topics: Sequence[str | Sequence[str] | None] | None = None,
    ) -> list[Log]:
        """Return the node's logs for blocks ``from_block`` .. ``to_block`` (inclusive).

        ``address`` is one address or a list; ``topics`` follows the JSON-RPC
        convention (``None`` matches anything at a position, a list means "any
        of"). The logs are the node's native dictionaries, unchanged.

        Checked here: every log lies in the range and matches the requested
        addresses and topics, none is ``removed``, hex fields are well formed,
        logs are strictly ascending by (block number, log index), and logs of
        one block share one block hash. Not checkable from one response: that
        the node returned *all* matching logs.
        """
        _check_range(from_block, to_block)
        address_filter = _address_filter(address)
        topic_filter = _topic_filter(topics)
        query: dict[str, Any] = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
        if address_filter is not None:
            query["address"] = address_filter
        if topic_filter is not None:
            query["topics"] = topic_filter
        result = self._rpc("eth_getLogs", [query])
        return _validate_logs(result, from_block, to_block, address_filter, topic_filter)

    def iter_logs(
        self,
        from_block: int,
        to_block: int,
        *,
        blocks_per_piece: int,
        address: str | Sequence[str] | None = None,
        topics: Sequence[str | Sequence[str] | None] | None = None,
    ) -> Iterator[LogPiece]:
        """Yield :class:`LogPiece` for consecutive fixed-size pieces of the range.

        Pieces are requested lazily, one per iteration, in ascending order. A
        failing piece raises and no later piece is requested; nothing is
        retried or skipped. To resume after an interruption, call again with
        ``from_block`` set to the first block after the last saved piece; the
        pieces then fall on the same boundaries as before.
        """
        pieces = split_block_range(from_block, to_block, blocks_per_piece)
        _address_filter(address)
        _topic_filter(topics)
        return self._iter_pieces(pieces, address, topics)

    def _iter_pieces(
        self,
        pieces: list[tuple[int, int]],
        address: str | Sequence[str] | None,
        topics: Sequence[str | Sequence[str] | None] | None,
    ) -> Iterator[LogPiece]:
        for first, last in pieces:
            yield LogPiece(first, last, self.get_logs(first, last, address=address, topics=topics))


def decode_order_filled(
    log: Log, *, contracts: frozenset[str] = POLYMARKET_EXCHANGE_CONTRACTS
) -> OrderFilled:
    """Decode one native log into :class:`OrderFilled`, or raise ``ValueError``.

    The log must come from one of ``contracts`` (any contract can emit an event
    with this signature), carry topic 0 :data:`ORDER_FILLED_TOPIC`, and have
    exactly the V2 layout: three indexed topics and seven 32-byte data words.
    """
    contract = _hex_data(_field(log, "address"), "address", size_bytes=20)
    if contract not in contracts:
        raise ValueError(f"log is not from a known Polymarket exchange contract: {contract}")
    topics = log.get("topics")
    if not isinstance(topics, list) or len(topics) != 4:
        raise ValueError("OrderFilled needs exactly 4 topics")
    topics = [_hex_data(topic, "topic", size_bytes=32) for topic in topics]
    if topics[0] != ORDER_FILLED_TOPIC:
        raise ValueError(f"not an OrderFilled V2 log: topic 0 is {topics[0]}")
    data = _hex_data(_field(log, "data"), "data")
    if len(data) != 2 + 64 * _ORDER_FILLED_DATA_WORDS:
        raise ValueError(f"OrderFilled data must be {_ORDER_FILLED_DATA_WORDS} 32-byte words")
    words = [data[2 + 64 * i : 2 + 64 * (i + 1)] for i in range(_ORDER_FILLED_DATA_WORDS)]
    side = int(words[0], 16)
    if side not in (SIDE_BUY, SIDE_SELL):
        raise ValueError(f"OrderFilled side must be 0 or 1, got {side}")
    timestamp = log.get("blockTimestamp")
    return OrderFilled(
        block_number=_quantity(log.get("blockNumber"), "blockNumber"),
        log_index=_quantity(log.get("logIndex"), "logIndex"),
        transaction_hash=_hex_data(_field(log, "transactionHash"), "transactionHash", size_bytes=32),
        transaction_index=_quantity(log.get("transactionIndex"), "transactionIndex"),
        contract=contract,
        order_hash=topics[1],
        maker=_topic_address(topics[2]),
        taker=_topic_address(topics[3]),
        side=side,
        token_id=int(words[1], 16),
        maker_amount_filled=int(words[2], 16),
        taker_amount_filled=int(words[3], 16),
        fee=int(words[4], 16),
        builder="0x" + words[5],
        metadata="0x" + words[6],
        block_timestamp=None if timestamp is None else _quantity(timestamp, "blockTimestamp"),
    )


def _topic_address(topic: str) -> str:
    if topic[2:26] != "0" * 24:
        raise ValueError(f"indexed address topic has nonzero padding: {topic}")
    return "0x" + topic[26:]


__all__ = [
    "LogPiece",
    "ORDER_FILLED_TOPIC",
    "OrderFilled",
    "POLYMARKET_EXCHANGE_CONTRACTS",
    "PolygonLogReader",
    "SIDE_BUY",
    "SIDE_SELL",
    "decode_order_filled",
    "split_block_range",
]
