# Polymarket Trade Data CLI

Pulled-apart tooling for inspecting OrderFilled events on the Polymarket CTF Exchange. The repository is a custom CLI plus a handful of helpers that normalize trades and follow top traders.

## Highlights

- Watch OrderFilled events straight from Bitquery’s GraphQL stream.
- Track individual traders, their total volume, unique assets, and recent fills.
- Inspect a specific asset’s trade history and derive an indicative price.
- Generate leaderboard-style tables for the most active traders/assets (configurable depth).
- Plug in your own `CopyTrader` implementation to simulate or execute follow-on trades.

> **Note:** The CLI exposes `--copy`/`copy-position` hooks, but you must supply your own safe `CopyTrader` module using Polymarket API before running them 

## Prerequisites

- python3 3.11+ (tested on 3.12).
- Bitquery OAuth token [Create an access token](https://account.bitquery.io/user/api_v2/access_tokens?utm_source=github&utm_medium=refferal&utm_campaign=polymarketCLI).
- Optional Polygon wallet secrets (seed phrase **or** private key) for signing.

Set up your environment:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuration

1. Copy the template:

   ```bash
   cp env.example .env
   ```

2. Populate the placeholders with your own values:
   - `OAUTH_TOKEN` — Bitquery token.
   - `SEED_PHRASE` **or** `PRIVATE_KEY` — supply exactly one; the CLI prefers the mnemonic.
   - `POLYMARKET_PROXY_ADDRESS` + `POLYMARKET_SIGNATURE_TYPE` — required only for proxy flows (Magic link, browser wallets). Leave blank for direct EOAs.
   - `POLYMARKET_API_KEY/SECRET/PASSPHRASE` — optional keys for sending authenticated orders.
   - `DEFAULT_COPY_AMOUNT_USD` — fallback USD amount for copy trade simulations.

All variables are loaded via `python-dotenv`. Do **not** check real secrets into git; `.env` is already ignored.

## CLI Usage

### Monitor a trader
```bash
python3 cli.py monitor --address 0x05c1882212a41aa8d7df5b70eebe03d9319345b7 --limit 20 --copy
```
![](/monitoraddress.png)

### Recent trades
```bash
python3 cli.py list-trades --limit 25
python3 cli.py list-trades --asset-id 0xasset...
```
![](/listtrades.png)

### Copy a Position
```bash
python3 cli.py copy-position --asset-id <ASSET_ID> --skip-question-details
python3 cli.py copy-position --asset-id <ASSET_ID> --execute  # requires working CopyTrader
```
![](/copyposition.png)

### Top Trader
```bash
python3 cli.py trader-summary --address 0x05c1882212a41aa8d7df5b70eebe03d9319345b7
python3 cli.py market-price --asset-id <ASSET_ID>
python3 cli.py top-traders --limit 30000 --top-traders 30 --top-assets 30
```
![](/toptraders.png)

## Project Layout

- `cli.py` — Command surface; coordinates Bitquery reads, table rendering, and CopyTrader hooks.
- `bitquery_client.py` — Simple requests-based GraphQL client with focused query builders.
- `position_tracker.py` — Converts events into `Position` dataclasses, aggregates stats, and exposes analytics helpers.
- `processing.py` — Value-extraction + normalization utilities shared by the tracker.
- `config.py` — Env loading, validation, runtime constants.
- `calculate.md` / `notes.md` — Supplemental documentation.
- `env.example` — Sanitised configuration scaffold (fill your own secrets).

## Security Checklist for Public Use

- Keep `.env` out of version control; only `env.example` should live in git.
- Rotate Bitquery and Polymarket keys regularly and scope them to least privilege.
- Review any custom `CopyTrader` module for rate limiting, slippage protection, and secret handling before turning on `--execute`.

## License

MIT
