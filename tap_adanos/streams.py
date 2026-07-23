"""Adanos market sentiment Singer streams."""

from __future__ import annotations

import decimal
import sys
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import quote

from tap_adanos.client import AdanosStream, OffsetPaginator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    import requests
    from singer_sdk.helpers.types import Context, Record
    from singer_sdk.pagination import BaseAPIPaginator


def _nullable(type_name: str) -> dict[str, Any]:
    return {"type": [type_name, "null"]}


COMMON_PROPERTIES: dict[str, Any] = {
    "asset_symbol": {"type": "string"},
    "asset_type": {"type": "string", "enum": ["stock", "crypto"]},
    "source": {"type": "string", "enum": ["reddit", "x", "news", "polymarket"]},
    "period_from": {"type": "string", "format": "date"},
    "period_to": {"type": "string"},
    "ticker": _nullable("string"),
    "symbol": _nullable("string"),
    "company_name": _nullable("string"),
    "name": _nullable("string"),
    "found": _nullable("boolean"),
    "buzz_score": _nullable("number"),
    "mentions": _nullable("integer"),
    "sentiment_score": _nullable("number"),
    "bullish_pct": _nullable("integer"),
    "bearish_pct": _nullable("integer"),
    "trend": _nullable("string"),
    "period_days": _nullable("integer"),
    "unique_posts": _nullable("integer"),
    "subreddit_count": _nullable("integer"),
    "total_upvotes": _nullable("integer"),
    "active_tickers": _nullable("integer"),
    "market_count": _nullable("integer"),
    "unique_traders": _nullable("integer"),
    "total_liquidity": _nullable("number"),
    "source_count": _nullable("integer"),
    "unique_tweets": _nullable("integer"),
    "trend_history": {"type": ["array", "null"], "items": {"type": ["number", "null"]}},
    "daily_trend": {"type": ["array", "null"], "items": {"type": "object"}},
    "top_mentions": {"type": ["array", "null"], "items": {"type": "object"}},
    "drivers": {"type": ["array", "null"], "items": {"type": "object"}},
}


def _schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": COMMON_PROPERTIES,
        "additionalProperties": True,
    }


class AssetSentimentStream(AdanosStream):
    """Detailed sentiment snapshots for configured stock or crypto symbols."""

    name = "asset_sentiment"
    schema = _schema()

    @property
    @override
    def partitions(self) -> list[dict[str, str]]:
        """Issue one detail request per configured symbol."""
        return [{"symbol": str(symbol).upper()} for symbol in self.config.get("symbols", [])]

    @override
    def endpoint_path(self, context: Context | None) -> str:
        """Route to the stock or crypto detail endpoint."""
        resource = "token" if self.asset_type == "crypto" else "stock"
        assert context is not None  # noqa: S101
        symbol = quote(str(context["symbol"]), safe="")
        return f"{self.endpoint_prefix}/{resource}/{symbol}"

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Apply the configured date window."""
        return self.window_params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[Record]:
        """Turn the detail object into one Singer record."""
        yield response.json(parse_float=decimal.Decimal)


class TrendingAssetsStream(AdanosStream):
    """Trending assets ordered by Adanos buzz score."""

    name = "trending_assets"
    schema = _schema()

    @override
    def endpoint_path(self, context: Context | None) -> str:
        """Return the effective trending endpoint."""
        return f"{self.endpoint_prefix}/trending"

    @override
    def get_new_paginator(self) -> BaseAPIPaginator[int]:
        """Page through all trending results."""
        return OffsetPaginator(int(self.config.get("page_size", 100)))

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        """Apply window and offset parameters."""
        return {
            **self.window_params,
            "limit": int(self.config.get("page_size", 100)),
            "offset": next_page_token or 0,
        }

    @override
    def parse_response(self, response: requests.Response) -> Iterable[Record]:
        """Emit each trending asset as a separate record."""
        yield from response.json(parse_float=decimal.Decimal)


class AssetComparisonStream(AdanosStream):
    """Comparable sentiment rows for all configured symbols."""

    name = "asset_comparison"
    schema = _schema()
    max_symbols_per_request = 10

    @property
    @override
    def partitions(self) -> list[dict[str, Any]]:
        """Split configured symbols into API-compatible comparison batches."""
        symbols = [str(symbol).upper() for symbol in self.config.get("symbols", [])]
        return [
            {"symbols": symbols[index : index + self.max_symbols_per_request]}
            for index in range(0, len(symbols), self.max_symbols_per_request)
        ]

    @override
    def endpoint_path(self, context: Context | None) -> str:
        """Return the effective comparison endpoint."""
        return f"{self.endpoint_prefix}/compare"

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Send configured symbols as the endpoint-specific query parameter."""
        assert context is not None  # noqa: S101
        key = "symbols" if self.asset_type == "crypto" else "tickers"
        return {
            **self.window_params,
            key: ",".join(context["symbols"]),
        }

    @override
    def parse_response(self, response: requests.Response) -> Iterable[Record]:
        """Emit each comparison item and retain the resolved period."""
        payload = response.json(parse_float=decimal.Decimal)
        key = "tokens" if self.asset_type == "crypto" else "stocks"
        for row in payload.get(key, []):
            row["period_days"] = payload.get("period_days")
            yield row


class MarketSentimentStream(AdanosStream):
    """Source-wide market sentiment snapshot."""

    name = "market_sentiment"
    primary_keys: ClassVar[tuple[str, ...]] = (
        "asset_type",
        "source",
        "period_from",
        "period_to",
    )
    schema = _schema()

    @override
    def endpoint_path(self, context: Context | None) -> str:
        """Return the effective market sentiment endpoint."""
        return f"{self.endpoint_prefix}/market-sentiment"

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Apply the configured date window."""
        return self.window_params

    @override
    def parse_response(self, response: requests.Response) -> Iterable[Record]:
        """Turn the market object into one Singer record."""
        yield response.json(parse_float=decimal.Decimal)
