"""Adanos Singer tap."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_adanos import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapAdanos(Tap):
    """Extract market sentiment and attention data from Adanos."""

    name = "tap-adanos"
    package_name = "tap-adanos"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Key",
            description="Adanos API key from https://adanos.org/register.",
        ),
        th.Property(
            "asset_type",
            th.StringType(nullable=False),
            default="stock",
            allowed_values=["stock", "crypto"],
            description="Asset class to extract. Crypto currently uses Reddit data.",
        ),
        th.Property(
            "stock_source",
            th.StringType(nullable=False),
            default="reddit",
            allowed_values=["reddit", "x", "news", "polymarket"],
            description="Sentiment source for stocks. Ignored for crypto.",
        ),
        th.Property(
            "symbols",
            th.ArrayType(th.StringType(nullable=False)),
            required=True,
            description="Stock tickers or crypto symbols used by detail and comparison streams.",
        ),
        th.Property(
            "start_date",
            th.DateType(nullable=False),
            required=True,
            description="Inclusive UTC start date within the account plan's lookback.",
        ),
        th.Property(
            "end_date",
            th.DateType,
            description="Inclusive UTC end date. Defaults to the current UTC date.",
        ),
        th.Property(
            "page_size",
            th.IntegerType(minimum=1, maximum=100),
            default=100,
            description="Page size for trending assets.",
        ),
        th.Property(
            "base_url",
            th.StringType(nullable=False),
            default="https://api.adanos.org",
            description="Adanos API base URL. Override for testing or private gateways.",
        ),
    ).to_dict()
    config_jsonschema["properties"]["symbols"]["minItems"] = 1

    @override
    def discover_streams(self) -> list[streams.AdanosStream]:
        """Return the four supported read-only streams."""
        return [
            streams.AssetSentimentStream(self),
            streams.TrendingAssetsStream(self),
            streams.AssetComparisonStream(self),
            streams.MarketSentimentStream(self),
        ]


if __name__ == "__main__":
    TapAdanos.cli()
