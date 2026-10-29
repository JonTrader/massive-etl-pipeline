-- Clean silver daily bars.
-- No dedupe: delete-then-copy already leaves one landing per trade_date in RAW.
-- A row_number() by extracted_at would hide a broken load. The uniqueness
-- test on (ticker, trade_date) catches duplicates instead.
-- Pass quality: ticker/close/open/high/low/volume present, high >= low, volume >= 0.
-- Explicit IS NOT NULL on compared columns so nulls cannot sneak past (SQL NULLs).

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
where "T" is not null
  and "o" is not null
  and "h" is not null
  and "l" is not null
  and "c" is not null
  and "v" is not null
  and "h" >= "l"
  and "v" >= 0
