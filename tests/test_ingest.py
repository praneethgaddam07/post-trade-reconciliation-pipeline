from decimal import Decimal

from posttrade.ingest.async_ingestor import AsyncIngestor
from posttrade.models import Side


def make_ingestor():
    return AsyncIngestor(
        ws_url="wss://example.invalid",
        symbols=["BTC-USD", "ETH-USD"],
        redis_url="redis://example.invalid:6379/0",
        stream_name="test-ticks",
    )


def test_normalize_ticker_message():
    ingestor = make_ingestor()
    msg = {
        "type": "ticker",
        "product_id": "BTC-USD",
        "price": "50000.12",
        "side": "buy",
        "time": "2026-01-01T00:00:00.000000Z",
    }
    tick = ingestor.normalize(msg, "ticker")
    assert tick is not None
    assert tick.symbol == "BTC-USD"
    assert tick.price == Decimal("50000.12")
    assert tick.side == Side.BUY
    assert tick.channel == "ticker"
    assert tick.sequence == 1


def test_normalize_match_message_uses_matches_channel():
    ingestor = make_ingestor()
    msg = {"type": "match", "product_id": "BTC-USD", "price": "50001.00", "size": "0.01", "side": "sell"}
    tick = ingestor.normalize(msg, "match")
    assert tick is not None
    assert tick.channel == "matches"
    assert tick.size == Decimal("0.01")
    assert tick.side == Side.SELL


def test_normalize_assigns_independent_sequences_per_symbol():
    ingestor = make_ingestor()
    btc1 = ingestor.normalize({"type": "ticker", "product_id": "BTC-USD", "price": "1"}, "ticker")
    eth1 = ingestor.normalize({"type": "ticker", "product_id": "ETH-USD", "price": "1"}, "ticker")
    btc2 = ingestor.normalize({"type": "ticker", "product_id": "BTC-USD", "price": "1"}, "ticker")
    assert btc1.sequence == 1
    assert eth1.sequence == 1
    assert btc2.sequence == 2


def test_normalize_missing_product_id_returns_none():
    ingestor = make_ingestor()
    tick = ingestor.normalize({"type": "ticker", "price": "1"}, "ticker")
    assert tick is None


def test_normalize_missing_price_returns_none():
    ingestor = make_ingestor()
    tick = ingestor.normalize({"type": "ticker", "product_id": "BTC-USD"}, "ticker")
    assert tick is None


def test_normalize_malformed_price_returns_none():
    ingestor = make_ingestor()
    tick = ingestor.normalize({"type": "ticker", "product_id": "BTC-USD", "price": "not-a-number"}, "ticker")
    assert tick is None


def test_normalize_malformed_size_is_dropped_but_tick_still_produced():
    ingestor = make_ingestor()
    tick = ingestor.normalize(
        {"type": "ticker", "product_id": "BTC-USD", "price": "1", "size": "garbage"}, "ticker"
    )
    assert tick is not None
    assert tick.size is None


def test_normalize_missing_time_falls_back_to_now():
    ingestor = make_ingestor()
    tick = ingestor.normalize({"type": "ticker", "product_id": "BTC-USD", "price": "1"}, "ticker")
    assert tick is not None
    assert tick.exchange_timestamp is not None
