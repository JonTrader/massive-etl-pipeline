-- Rows that fail silver quality. Bronze/S3 is untouched; this table is the
-- explainable failed set. Include IS NULL on compared columns so every RAW
-- row lands in either stg_daily_bars or quarantine (SQL three-valued logic).
-- Failures: null ticker/open/high/low/close/volume, high < low, or volume < 0.
-- Same renames and casts as stg_daily_bars; no dedupe (see that model).

select
    cast("T" as varchar) as ticker,
    cast("o" as float) as open,
    cast("h" as float) as high,
    cast("l" as float) as low,
    cast("c" as float) as close,
    cast("v" as float) as volume,
    cast("vw" as float) as vwap,
    cast("t" as number) as window_start_epoch_ms,
    cast("n" as number) as transactions,
    trade_date,
    extracted_at
from {{ source('raw', 'DAILY_BARS') }}
where "T" is null
   or "o" is null
   or "h" is null
   or "l" is null
   or "c" is null
   or "v" is null
   or "h" < "l"
   or "v" < 0
