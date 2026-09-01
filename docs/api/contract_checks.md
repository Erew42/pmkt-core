# Contract checks

`scripts/contract_check.py` performs a minimal contract check against the public
Gamma and CLOB APIs to catch breaking drift. It is a live network check, so it
can fail because of upstream API changes, temporary network failures, rate
limits, or a lack of currently active order books in the scanned markets.

What it checks:
- Gamma `/markets` returns JSON and yields a CLOB token id.
- CLOB `/book`, `/price`, `/midpoint`, and `/prices-history` return JSON with
  stable keys; `/prices-history` is queried with
  `market=<token_id>&interval=1d`, requires a `history` key, and is skipped only
  on 404.

By default it scans up to 3 pages of Gamma `/markets` to find a token with an
active order book; override with `--max-pages`.

Run:
```bash
python scripts/contract_check.py
python scripts/contract_check.py --json
```

Options:

- `--gamma-base-url`: Gamma API base URL. Default:
  `https://gamma-api.polymarket.com`.
- `--clob-base-url`: CLOB API base URL. Default:
  `https://clob.polymarket.com`.
- `--timeout`: per-request timeout in seconds. Default: `20.0`.
- `--max-retries`: maximum total request attempts for transient failures,
  including the initial attempt. Default: `4`.
- `--max-pages`: Gamma `/markets` pages to scan for a token with an active order
  book. Default: `3`.
- `--json`: print a machine-readable JSON summary instead of the text summary.

Expected output is a timestamped summary, the token id used for CLOB checks, and
one line per checked endpoint. A skipped `/prices-history` check with 404 is not
treated as failure because not every active token has historical data available
through that endpoint.
