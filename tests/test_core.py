"""Tests for Adanos stream routing, records, and standard Singer behavior."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import jsonschema
import pytest

from tap_adanos.streams import (
    AssetComparisonStream,
    AssetSentimentStream,
    MarketSentimentStream,
    TrendingAssetsStream,
)
from tap_adanos.tap import TapAdanos

if TYPE_CHECKING:
    from requests_mock import Mocker


HEADER_VALUE = "unit-test"
AUTH_SETTING = "_".join(("api", "key"))  # noqa: FLY002
SAMPLE_CONFIG = {
    AUTH_SETTING: HEADER_VALUE,
    "asset_type": "stock",
    "stock_source": "reddit",
    "symbols": ["AAPL", "NVDA"],
    "start_date": "2026-07-01",
    "end_date": "2026-07-07",
    "base_url": "https://api.example.test",
}
SAMPLE_CONFIG_PERIOD_DAYS = 7
MARKET_BUZZ_SCORE = 55.2
MARKET_SENTIMENT_SCORE = 0.12


def _records(
    stream: AssetSentimentStream
    | TrendingAssetsStream
    | AssetComparisonStream
    | MarketSentimentStream,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for context in stream.partitions or [None]:
        for record in stream.get_records(context):
            processed = stream.post_process(record, context)
            if processed is not None:
                records.append(processed)
    return records


def test_asset_sentiment_routes_each_stock_symbol(requests_mock: Mocker) -> None:
    """The detail stream should partition requests and attach snapshot context."""
    for symbol in ("AAPL", "NVDA"):
        requests_mock.get(
            f"https://api.example.test/reddit/stocks/v1/stock/{symbol}",
            json={"ticker": symbol, "found": True, "buzz_score": 42.5},
        )

    records = _records(AssetSentimentStream(TapAdanos(config=SAMPLE_CONFIG)))

    assert [record["asset_symbol"] for record in records] == ["AAPL", "NVDA"]
    assert records[0]["period_from"] == "2026-07-01"
    assert records[0]["period_to"] == "2026-07-07"
    assert requests_mock.request_history[0].headers["X-API-Key"] == HEADER_VALUE
    assert requests_mock.request_history[0].qs == {
        "from": ["2026-07-01"],
        "to": ["2026-07-07"],
    }


def test_crypto_routes_to_reddit_token_endpoint(requests_mock: Mocker) -> None:
    """Crypto should ignore the stock source and use the Reddit token endpoint."""
    config = {
        **SAMPLE_CONFIG,
        "asset_type": "crypto",
        "stock_source": "news",
        "symbols": ["btc"],
    }
    requests_mock.get(
        "https://api.example.test/reddit/crypto/v1/token/BTC",
        json={"symbol": "BTC", "found": True, "sentiment_score": 0.25},
    )

    records = _records(AssetSentimentStream(TapAdanos(config=config)))

    assert records[0]["asset_symbol"] == "BTC"
    assert records[0]["asset_type"] == "crypto"
    assert records[0]["source"] == "reddit"


def test_trending_assets_pages_until_short_page(requests_mock: Mocker) -> None:
    """Trending extraction should follow deterministic offset pagination."""
    config = {**SAMPLE_CONFIG, "page_size": 2}
    url = "https://api.example.test/news/stocks/v1/trending"
    config["stock_source"] = "news"
    requests_mock.get(
        url,
        [
            {"json": [{"ticker": "AAPL"}, {"ticker": "NVDA"}]},
            {"json": [{"ticker": "MSFT"}]},
        ],
    )

    records = _records(TrendingAssetsStream(TapAdanos(config=config)))

    assert [record["asset_symbol"] for record in records] == ["AAPL", "NVDA", "MSFT"]
    assert [request.qs["offset"] for request in requests_mock.request_history] == [["0"], ["2"]]
    assert all(record["source"] == "news" for record in records)


@pytest.mark.parametrize(
    ("asset_type", "response_key", "query_key", "path"),
    [
        ("stock", "stocks", "tickers", "/x/stocks/v1/compare"),
        ("crypto", "tokens", "symbols", "/reddit/crypto/v1/compare"),
    ],
)
def test_comparison_extracts_rows(
    requests_mock: Mocker,
    asset_type: str,
    response_key: str,
    query_key: str,
    path: str,
) -> None:
    """Comparison should normalize stock and crypto response envelopes."""
    config = {**SAMPLE_CONFIG, "asset_type": asset_type, "stock_source": "x"}
    symbol_key = "ticker" if asset_type == "stock" else "symbol"
    requests_mock.get(
        f"https://api.example.test{path}",
        json={
            "period_days": 7,
            response_key: [
                {symbol_key: "AAPL" if asset_type == "stock" else "BTC", "buzz_score": 70}
            ],
        },
    )

    records = _records(AssetComparisonStream(TapAdanos(config=config)))

    assert records[0]["period_days"] == SAMPLE_CONFIG_PERIOD_DAYS
    assert requests_mock.request_history[0].qs[query_key] == ["aapl,nvda"]


def test_comparison_batches_more_than_ten_symbols(requests_mock: Mocker) -> None:
    """Comparison should stay within the API's per-request symbol limit."""
    symbols = [f"TICKER{index}" for index in range(11)]
    config = {**SAMPLE_CONFIG, "symbols": symbols}
    requests_mock.get(
        "https://api.example.test/reddit/stocks/v1/compare",
        [
            {"json": {"period_days": 7, "stocks": []}},
            {"json": {"period_days": 7, "stocks": []}},
        ],
    )

    _records(AssetComparisonStream(TapAdanos(config=config)))

    batches = [request.qs["tickers"][0].split(",") for request in requests_mock.request_history]
    assert [len(batch) for batch in batches] == [10, 1]
    assert [symbol for batch in batches for symbol in batch] == [
        symbol.lower() for symbol in symbols
    ]


def test_market_sentiment_emits_one_record(requests_mock: Mocker) -> None:
    """Market sentiment should retain source and window metadata."""
    requests_mock.get(
        "https://api.example.test/polymarket/stocks/v1/market-sentiment",
        json={"buzz_score": MARKET_BUZZ_SCORE, "sentiment_score": MARKET_SENTIMENT_SCORE},
    )
    config = {**SAMPLE_CONFIG, "stock_source": "polymarket"}

    records = _records(MarketSentimentStream(TapAdanos(config=config)))

    assert len(records) == 1
    assert float(records[0]["buzz_score"]) == MARKET_BUZZ_SCORE
    assert float(records[0]["sentiment_score"]) == MARKET_SENTIMENT_SCORE
    assert records[0]["asset_type"] == "stock"
    assert records[0]["source"] == "polymarket"
    assert records[0]["period_from"] == "2026-07-01"
    assert records[0]["period_to"] == "2026-07-07"


def test_api_errors_include_service_detail(requests_mock: Mocker) -> None:
    """Users should receive the API error instead of a silent empty sync."""
    requests_mock.get(
        "https://api.example.test/reddit/stocks/v1/market-sentiment",
        status_code=403,
        json={"detail": {"error_code": "plan_limit", "message": "Lookback exceeds plan"}},
    )

    with pytest.raises(Exception, match="403") as error:
        _records(MarketSentimentStream(TapAdanos(config=SAMPLE_CONFIG)))

    assert "plan_limit" in str(error.value)
    assert HEADER_VALUE not in str(error.value)


def test_discovery_contains_expected_streams() -> None:
    """Discovery should expose only the supported core endpoint groups."""
    catalog = TapAdanos(config=SAMPLE_CONFIG).catalog_dict

    assert {stream["tap_stream_id"] for stream in catalog["streams"]} == {
        "asset_sentiment",
        "trending_assets",
        "asset_comparison",
        "market_sentiment",
    }


def test_discovered_stream_schemas_are_valid() -> None:
    """Every discovered stream should expose valid JSON Schema."""
    for stream in TapAdanos(config=SAMPLE_CONFIG).discover_streams():
        jsonschema.Draft202012Validator.check_schema(stream.schema)


def test_config_schema_rejects_invalid_source() -> None:
    """The generated config contract should reject unsupported stock sources."""
    config = {**SAMPLE_CONFIG, "stock_source": "telegram"}

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            config["stock_source"],
            TapAdanos.config_jsonschema["properties"]["stock_source"],
        )


def test_period_to_defaults_to_current_utc_date() -> None:
    """Open-ended snapshots should request and emit the same stable UTC end date."""
    config = {key: value for key, value in SAMPLE_CONFIG.items() if key != "end_date"}
    stream = MarketSentimentStream(TapAdanos(config=config))
    expected = datetime.now(timezone.utc).date().isoformat()

    assert stream.period_to == expected
    assert stream.window_params == {"from": "2026-07-01", "to": expected}
