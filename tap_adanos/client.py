"""REST client shared by all Adanos streams."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from functools import cached_property
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, ClassVar

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseAPIPaginator, SinglePagePaginator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context, Record


class OffsetPaginator(BaseAPIPaginator[int]):
    """Advance through Adanos list endpoints using limit and offset."""

    def __init__(self, page_size: int) -> None:
        """Create an offset paginator."""
        self.page_size = page_size
        super().__init__(start_value=0)

    @override
    def get_next(self, response: requests.Response) -> int | None:
        records = response.json()
        if not isinstance(records, list) or len(records) < self.page_size:
            return None
        return self.current_value + self.page_size


class AdanosStream(RESTStream):
    """Base stream for the Adanos Market Sentiment API."""

    primary_keys: ClassVar[tuple[str, ...]] = (
        "asset_symbol",
        "source",
        "period_from",
        "period_to",
    )
    replication_key = None
    path = ""
    extra_retry_statuses = (
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    )

    @property
    @override
    def url_base(self) -> str:
        """Return the configured Adanos API root."""
        return str(self.config.get("base_url", "https://api.adanos.org")).rstrip("/")

    @property
    @override
    def authenticator(self) -> APIKeyAuthenticator:
        """Authenticate with the documented Adanos API key header."""
        return APIKeyAuthenticator(
            key="X-API-Key",
            value=self.config["api_key"],
            location="header",
        )

    @property
    def asset_type(self) -> str:
        """Return the configured asset class."""
        return str(self.config.get("asset_type", "stock"))

    @property
    def source(self) -> str:
        """Return the effective sentiment source."""
        if self.asset_type == "crypto":
            return "reddit"
        return str(self.config.get("stock_source", "reddit"))

    @property
    def endpoint_prefix(self) -> str:
        """Return the source-specific v1 endpoint prefix."""
        if self.asset_type == "crypto":
            return "/reddit/crypto/v1"
        return f"/{self.source}/stocks/v1"

    @property
    def window_params(self) -> dict[str, str]:
        """Return the configured inclusive UTC date window."""
        return {"from": self.config["start_date"], "to": self.period_to}

    def endpoint_path(self, context: Context | None) -> str:
        """Return the API path for this stream."""
        raise NotImplementedError

    @override
    def get_url(self, context: Context | None) -> str:
        """Build a URL from the configured API root and dynamic endpoint path."""
        return f"{self.url_base}{self.endpoint_path(context)}"

    @cached_property
    def period_to(self) -> str:
        """Return a stable period end marker for emitted records."""
        return str(self.config.get("end_date") or datetime.now(timezone.utc).date().isoformat())

    @override
    def get_new_paginator(self) -> BaseAPIPaginator[Any]:
        """Use one request unless a list stream opts into offset pagination."""
        return SinglePagePaginator()

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record:
        """Attach extraction context needed to identify each snapshot."""
        row["asset_type"] = self.asset_type
        row["source"] = self.source
        row["period_from"] = self.config["start_date"]
        row["period_to"] = self.period_to
        if context and "symbol" in context:
            row["asset_symbol"] = str(context["symbol"]).upper()
        elif symbol := row.get("ticker") or row.get("symbol"):
            row["asset_symbol"] = str(symbol).upper()
        return row
