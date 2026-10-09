# Polymarket fill data: analyst reference

This document describes the current `poly_data_storage` decoder and the 15
columns written to `polymarket_orders_YYYY_MM_DD.parquet`. The filename identifies
the UTC day: timestamps are in `[00:00:00 UTC, next day 00:00:00 UTC)`.

## What one row represents

One row is one Polygon `OrderFilled` log from a supported CLOB exchange. It is
an executed fill, not an order submission, an order-book snapshot, or necessarily
one independent economic trade. One transaction can contain several fills.
An order can also be partially filled in multiple logs and transactions.

The collector retains both maker-order fills and taker-order aggregate fills.
The event's `maker` field identifies the owner of the order represented by that
row, including when that order was the taker order in a matching operation.

Coverage includes the five deployments listed in [README.md](README.md).
Earlier AMM trades, other chains, market/event descriptions, transfers, and
redemptions are outside this dataset.

## Columns

Types below are the stored Arrow/Parquet types. Addresses and hashes are lowercase
hexadecimal strings beginning with `0x`.

| Column | Type | Meaning and interpretation |
| --- | --- | --- |
| `block_number` | `int64` | Polygon block containing the fill. Use with `log_index` for on-chain ordering. |
| `timestamp` | `int64` | Block header Unix timestamp in seconds since `1970-01-01T00:00:00Z`. This is seconds, not milliseconds. All fills in a block share this timestamp. |
| `block_time` | `string` | The same block timestamp as an ISO 8601 UTC string, such as `2026-10-08T00:00:01+00:00`. It is stored as text, not a native timestamp column. |
| `tx_hash` | `string` | 32-byte transaction hash. Several rows can have the same transaction hash. |
| `log_index` | `int32` | The log's index within its block, not its transaction. Indices can have gaps because other event types are excluded. |
| `order_hash` | `string` | 32-byte hash identifying the filled order. It can repeat across partial fills; it is not a unique row identifier. |
| `maker` | `string` | 20-byte address in the event's maker field: the owner of the order represented by this fill. It may be a proxy/contract wallet; it does not establish a person's identity. |
| `taker` | `string` | 20-byte address in the event's taker field. It can be the counterparty order owner or the emitting exchange for an aggregate fill. It is not necessarily the transaction sender. |
| `is_taker` | `bool` | Exactly `taker == emitting_exchange_address`. `true` identifies the exchange-facing taker-order aggregate pattern. This flag does not mean that the address in `taker` is a human trader. |
| `is_sell` | `bool` | Side of the order represented by this row: `true` means its owner sells outcome shares for collateral; `false` means its owner buys shares with collateral. This applies to `maker`, including aggregate rows, not to the address in `taker`. |
| `token_id` | `string` | Outcome identifier as a decimal string: CTF token ID for V1/V2, Protocol V2 position ID for V3. Keep it as text to avoid integer overflow or floating-point corruption. Market title and YES/NO outcome require external mapping. |
| `fee` | `float64` | Event fee integer divided by `1,000,000`. Units depend on version and side: see the fee table below. It is an amount, not a percentage or basis-point rate. |
| `amount_usd` | `float64` | Gross collateral amount in the fill, divided by `1,000,000`. Despite the name, this is nominal collateral units, not a measured USD exchange-rate conversion or a fee-adjusted cash flow. |
| `shares` | `float64` | Gross outcome amount in the fill, divided by `1,000,000`. Fractional shares are possible. This is not automatically the wallet's net share change after fees. |
| `price` | `float64` | Gross collateral per share: `amount_usd / shares`. Computed from raw integers before floating-point scaling. `null` when the raw share amount is zero. It is an execution ratio, not a quoted midpoint or fee-adjusted price. |

Only `price` is intentionally produced as null by the decoder. The Arrow schema
allows nulls in every field, but successful current decoding supplies all other
fields. Historical files produced by other collector revisions need separate
validation.

## Side and amount normalization

The decoder normalizes the event amounts as follows:

| Order side (`is_sell`) | Gross collateral (`amount_usd`) | Gross outcome quantity (`shares`) |
| --- | --- | --- |
| BUY (`false`) | Maker amount filled / `1,000,000` | Taker amount filled / `1,000,000` |
| SELL (`true`) | Taker amount filled / `1,000,000` | Maker amount filled / `1,000,000` |

In V1, the collateral asset ID is zero; its position in the asset pair determines
the side. In V2/V3, the side field is decoded explicitly: 0 is BUY and 1 is SELL.
The amount fields above describe the event; they do not establish a complete
transaction-level cash-flow ledger.

## Fee units

| Deployment version | BUY fee | SELL fee |
| --- | --- | --- |
| V1 | Outcome shares | Collateral units |
| V2/V3 | Collateral units | Collateral units |

Do not sum `fee` across all rows as USD or subtract it uniformly from
`amount_usd`. The output omits the emitting exchange address and version, so the
fee unit cannot always be recovered from these columns alone. A transaction can
contain activity from multiple exchanges; accurate enrichment should recover
the emitting address using `(tx_hash, log_index)` and the deployment registry.
Dates alone are insufficient because deployment versions overlap.

The V1 fee distinction is documented in
[Polymarket's fee calculator](https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/libraries/CalculatorHelper.sol).
The V2 event and collateral fee settlement are shown in
[Events.sol](https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/Events.sol)
and [Trading.sol](https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/Trading.sol).

## Identity, ordering, and aggregation

- Within this Polygon dataset, use `(tx_hash, log_index)` to identify a fill log.
  Include chain ID if combining with other chains. Do not deduplicate by
  `order_hash` or `tx_hash` alone.
- Sort by `(block_number, log_index)` for deterministic blockchain order.
  Timestamp alone does not order fills within a block.
- For a maker-fill view, filter `is_taker == false`. For a taker aggregate view,
  filter `is_taker == true`. Choose the view appropriate to the analysis rather
  than adding both together as independent volume. Matching can also mint or
  merge complementary positions, so define token-level versus market-level
  volume explicitly.
- For a chosen, consistent set of rows with positive total shares, a
  share-weighted execution price is `sum(amount_usd) / sum(shares)`. Group by a
  mapped outcome/position; averaging `price` without weighting answers a
  different question.
- These rows record fills, not market resolutions, payouts, complete wallet
  inventory, or profit and loss. Those analyses need additional data.

The maker/aggregate event distinction and mint/merge matching are visible in
[Polymarket's V1 trading implementation](https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/mixins/Trading.sol).

## Example from an existing file

The first row inspected in `polymarket_orders_2026_10_08.parquet` contains:

| Field | Value |
| --- | --- |
| `block_number` | `95141601` |
| `timestamp` | `1791417601` |
| `block_time` | `2026-10-08T00:00:01+00:00` |
| `is_taker` | `false` |
| `is_sell` | `false` |
| `amount_usd` | `1.26` |
| `shares` | `2.8` |
| `price` | `0.45` |
| `fee` | `0.0` |

This is a maker-order BUY fill: 2.8 gross shares for 1.26 nominal collateral
units, at 0.45 collateral units per share. Its block time is also
`07_10_2026 19:00:01 EST` using fixed UTC-05:00. New York civil time on that date
uses EDT, so it is a different conversion from the requested fixed EST display.

## Precision and completion

`fee`, `amount_usd`, `shares`, and `price` use binary floating-point values.
Small rounding differences are expected. Exact raw event integers are not
preserved; use tolerance-based comparisons and recover raw logs for exact
integer accounting. The schema also omits exchange provenance, builder,
metadata, condition IDs, and event/market descriptions.

The footer key `poly_data_storage.completion` records the collector revision,
deployment registry, finalized boundaries, and reconciled block/log/row counts.
Both existing files inspected for this report have the documented schema and a
completion marker for revision 2: October 7 has 3,471,338 rows and October 8 has
3,661,642 rows. A completion marker certifies the collector's processing of
returned data; it does not independently prove the provider omitted no logs.

Implementation references: [decoder](blockchain_decoder.py),
[Parquet schema](storage.py), and [deployment registry](constants.py).
