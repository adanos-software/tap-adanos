# tap-adanos

`tap-adanos` is a Singer tap for the [Adanos Market Sentiment API](https://api.adanos.org/docs), built with the [Meltano Singer SDK](https://sdk.meltano.com).

It extracts stock sentiment from Reddit, X / FinTwit, financial news, and Polymarket, plus Reddit crypto sentiment. The tap exposes four warehouse-friendly snapshot streams:

- `asset_sentiment`: one detailed sentiment record per configured symbol.
- `trending_assets`: paginated assets ordered by current buzz score.
- `asset_comparison`: normalized rows from the Adanos comparison endpoint.
- `market_sentiment`: one source-wide market sentiment record.

Raw mention streams are intentionally excluded because those endpoints require the Professional plan. Every core stream works with Free, Hobby, and Professional API keys when its date window is within the plan's lookback.

## Installation

Install from PyPI after the first release:

```bash
uv tool install tap-adanos
```

Install from GitHub during development:

```bash
uv tool install git+https://github.com/adanos-software/tap-adanos.git@main
```

## Configuration

| Setting | Required | Default | Description |
| --- | --- | --- | --- |
| `api_key` | Yes | | Adanos API key from [adanos.org/register](https://adanos.org/register). |
| `asset_type` | No | `stock` | `stock` or `crypto`. Crypto currently uses Reddit data. |
| `stock_source` | No | `reddit` | Stock source: `reddit`, `x`, `news`, or `polymarket`. |
| `symbols` | Yes | | One or more symbols used by `asset_sentiment` and `asset_comparison`. |
| `start_date` | Yes | | Inclusive UTC date in `YYYY-MM-DD` format. |
| `end_date` | No | Current UTC date | Inclusive UTC date in `YYYY-MM-DD` format. |
| `page_size` | No | `100` | Trending page size from 1 to 100. |
| `base_url` | No | `https://api.adanos.org` | API root, primarily useful for tests or private gateways. |

The API currently permits up to 30 days of lookback for Free accounts, 90 days for Hobby, and 365 days for Professional. Quotas and rate limits remain attached to the supplied Adanos API key.

Example non-secret settings:

```json
{
  "asset_type": "stock",
  "stock_source": "reddit",
  "symbols": ["AAPL", "NVDA"],
  "start_date": "2026-07-01",
  "end_date": "2026-07-07"
}
```

Provide `api_key` through Meltano's secret configuration or another secure runtime mechanism rather than storing it in this file.

Discover and run the tap:

```bash
tap-adanos --config config.json --discover > catalog.json
tap-adanos --config config.json --catalog catalog.json
```

Use `--select` or a Meltano catalog to enable only the streams needed by a pipeline.

## Meltano

```yaml
plugins:
  extractors:
    - name: tap-adanos
      namespace: tap_adanos
      pip_url: tap-adanos
      executable: tap-adanos
      config:
        asset_type: stock
        stock_source: reddit
        symbols: [AAPL, NVDA]
        start_date: "2026-07-01"
```

Set the key outside source control:

```bash
meltano config tap-adanos set api_key "$ADANOS_API_KEY"
meltano run tap-adanos target-jsonl
```

## Development

Requires Python 3.10+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run ruff check .
uv run mypy tap_adanos tests
uv run tap-adanos --about --format=json
```

## License

Apache-2.0
