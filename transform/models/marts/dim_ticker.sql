-- One row per ticker seen in silver. Ticker is the natural key.
-- No company name or exchange; those need a reference feed this pipeline does not pull.

select
    ticker,
    min(trade_date) as first_trade_date,
    max(trade_date) as last_trade_date
from {{ ref('stg_daily_bars') }}
group by ticker
