# Polygon CLOB fill collector

Run with the project's uv environment:

```sh
uv run poly-fetch --start 2025-01-01 --end 2026-07-31
```

Dates are inclusive UTC days. Settings remain in `constants.py`. Output defaults
to the project's `data/` directory, independent of the launch directory. Each
completed day is `polymarket_orders_YYYY_MM_DD.parquet` with the original 15
columns and Arrow types. There are no new runtime dependencies.

## Historical coverage

The registry in `constants.py` covers these Polygon CLOB exchanges:

| Deployment | Address | Event data |
| --- | --- | --- |
| CTF V1 | `0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e` | Five words |
| Negative-risk V1 | `0xc5d563a36ae78145c45a50134d48a1215220f80a` | Five words |
| CTF V2 | `0xe111180000d2663c0091e4f400237545b87b996b` | Seven words |
| Negative-risk V2 | `0xe2222d279d744050d28e00520010520000310f59` | Seven words |
| ExchangeV3 | `0xe3333700ca9d93003f00f0f71f8515005f6c00aa` | Seven words |

Every request includes all supported deployments, grouped by event signature.
`blockchain_decoder.py` selects the layout from the emitting contract and topic.
This captures overlap between versions without estimated deployment dates or
observed last-fill cutoffs. Adding a deployment requires verifying its ABI and
updating the registry; bump `COLLECTION_VERSION` when collection semantics change.

Coverage is CLOB `OrderFilled` events from these deployments. Earlier AMM trades,
non-Polygon activity, transfers, redemptions, and market/event metadata are not
collected. Selecting complete lifetime histories for events requires the separate
catalog/analysis application to identify the additional dates to fetch.

## Meaning of the preserved columns

- Every distinct fill log is retained, including taker aggregate fills.
  `is_taker` means the event's taker equals its emitting exchange. Maker and taker
  aggregate amounts must not simply be summed together as independent volume.
- `token_id` is a CTF token ID for V1/V2 and a Protocol V2 position ID for V3.
- `amount_usd` retains the existing name and represents nominal collateral units,
  not a conversion using an external USD exchange rate. `price` is the gross
  collateral/share ratio; it is null when shares are zero.
- `fee` is the native event fee divided by 1,000,000. V1 BUY fees are outcome
  shares; V1 SELL fees and V2/V3 fees are collateral. Do not treat all fees as USD.
- Float columns retain the original analytical precision. This schema does not
  preserve exact raw integers, builder/metadata, or per-row exchange provenance.

Sources: [legacy deployments](https://github.com/Polymarket/py-clob-client/blob/main/py_clob_client/config.py),
[current deployments](https://docs.polymarket.com/resources/contracts),
[V1 event](https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/interfaces/ITrading.sol),
[V1 fee units](https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/libraries/CalculatorHelper.sol),
[V2 event](https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/Events.sol).

## Completion and retries

Midnights come from SQD's timestamp endpoint and are checked against neighboring
finalized headers. A day scans from its first block through the block immediately
before the next day's first block. No expected block counts or estimated ranges
are used. Verified shared midnights are cached during a run.

The next-day boundary must be finalized. Incomplete/current/future days are
reported as NOT READY and are not published. The command exits nonzero if any day
fails or is not ready.

`includeAllBlocks` makes quiet blocks explicit. One cursor continues after every
complete block, including header-only responses. Stream failures resume at the
first unprocessed block. Malformed records are never silently discarded. HTTP
success does not reset the failure budget; validated block progress does.
Retries honour `Retry-After` with bounded backoff.

A file is published only after block, log, and written-row counts reconcile.
Completion details are stored in its Parquet footer, without adding columns.
Existing files are skipped only if their footer matches this collector revision
and coverage. Older files are reported as unverified; use a separate output
folder or explicitly set `REPLACE=True` to rebuild them. A failed rebuild leaves
the old file intact.

Per-day advisory locks prevent competing collectors on macOS/Linux from writing
the same day. Hidden lock files are intentionally retained. Temporary files use
unique names; a forcibly killed process may leave an unused temporary file.

These checks establish complete processing of SQD's returned history; they cannot
detect a provider omitting an otherwise valid matching log.

## Focused verification

```sh
uv run ruff check src/poly_data_storage
uv run ruff format --check src/poly_data_storage
uv run ty check src/poly_data_storage
uv run python -m unittest discover -s src/poly_data_storage/tests -v
```
