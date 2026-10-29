-- Daily bar facts at grain ticker + trade_date.
-- Unadjusted OHLCV, VWAP, and transaction count from silver.
-- window_start_epoch_ms stays in staging only; not a gold measure.

select
    ticker,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    vwap,
    transactions,
    extracted_at
from {{ ref('stg_daily_bars') }}
