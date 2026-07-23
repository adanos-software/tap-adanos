# AGENTS.md

## Scope

This repository contains `tap-adanos`, a read-only Singer tap for the Adanos Market Sentiment API. Keep changes focused on extraction, normalization, Singer compatibility, and Meltano Hub metadata. The tap must not contain trading or portfolio logic.

## API contract

- Treat `https://api.adanos.org/openapi.json` as the public API source of truth.
- Authenticate only with the `X-API-Key` header. Never log or commit real keys.
- Stocks support Reddit, X / FinTwit, News, and Polymarket. Crypto currently supports Reddit.
- Core streams must remain usable by Free, Hobby, and Professional accounts when the configured window is within the account lookback.
- Do not add Professional-only endpoints, such as raw mentions, to an existing core stream.
- Preserve source-specific response fields through permissive schemas while keeping stable normalized metadata explicit.

## Singer invariants

- Keep `config_jsonschema`, `meltano.yml`, and README settings aligned.
- Use the Meltano Singer SDK HTTP, authentication, pagination, and retry mechanisms.
- Emit `asset_type`, `source`, `period_from`, and `period_to` on every record.
- Emit `asset_symbol` for asset-level records.
- Keep snapshot streams in full-table mode unless the API exposes a durable append-only cursor.
- New streams require schemas, discovery registration, mocked request tests, and documentation.

## Verification

Run before committing:

```bash
uv sync --all-groups
uv run pytest -q
uvx ruff format --check .
uvx ruff check .
uv run mypy tap_adanos tests
uv run ty check tap_adanos tests
uv build
uvx pre-commit run --all-files
```

Tests must not require a live Adanos API key or make network requests to production.
