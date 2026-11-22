# Stock Review UI contract (v1)

The UI reads `intradaytrading.stock_review_daily`; it never parses
`report_markdown` or recalculates indicators.

## Queries

```sql
SELECT * FROM intradaytrading.stock_review_daily
WHERE exchange = :exchange AND symbol = :symbol AND trade_date = :trade_date;

SELECT trade_date FROM intradaytrading.stock_review_daily
WHERE exchange = :exchange AND symbol = :symbol
ORDER BY trade_date DESC;
```

`volume_context_json`, `comparison_json`, `next_session_watch_json`,
`stock_coverage_json`, and `benchmark_coverage_json` carry
`schema_version: stock-review-v1`. Zone documents carry their versioned rule
in `rule` (`v1_2_swing_2_distinct_reactions`). Dates use `YYYY-MM-DD`;
timestamps are ISO-8601 with `+05:30` offset.

## Field mapping

| UI section | Stored fields |
|---|---|
| Header | `exchange`, `symbol`, `trade_date`, `stock_coverage_json.header` |
| Price strip | `session_high`, `session_low`, `session_close`, plus `session_open` |
| Stock movement | `volume_context_json.movement` |
| Support/resistance | `support_json`, `resistance_json` |
| Volume at support | `volume_context_json` excluding `movement` |
| Benchmark comparison | `benchmark_status`, `benchmark_identity`, `benchmark_contract`, `benchmark_expiry`, `comparison_json`, `benchmark_coverage_json` |
| Next session | `next_session_watch_json` |
| Provenance | `stock_coverage_json.git_revision`, `csv_path`, `sha256` |

## Availability states

Zone and volume documents use `state: AVAILABLE` or `UNAVAILABLE`. An
unavailable document always contains `reason`. `benchmark_status` can be
`VERIFIED`, `UNVERIFIED`, or `UNAVAILABLE`; v1 stores `UNVERIFIED` whenever
the aligned NIFTY series has no contract identity.

## Example read shape

```json
{
  "exchange": "NSE",
  "symbol": "BHARTIARTL",
  "trade_date": "2026-11-20",
  "analysis_version": "stock-review-v1",
  "status": "COMPLETE",
  "session_high": 0.0,
  "session_low": 0.0,
  "session_close": 0.0,
  "volume_context_json": {"schema_version": "stock-review-v1", "movement": {"label": "ADVANCE"}},
  "stock_coverage_json": {"git_revision": "<40-char SHA>", "csv_path": "Output/20261120/BHARTIARTL.csv"}
}
```
