-- Derived session measures at the same grain as fact_daily_bar (ticker + trade_date).
-- Linked to the bar fact via shared dims dim_ticker and dim_date (conformed keys).
-- dollar_volume uses close * volume (locked). Prior-close fields are null on a
-- ticker's first day in the loaded history.

with bars as (
    select
        ticker,
        trade_date,
        open,
        high,
        low,
        close,
        volume,
        lag(close) over (
            partition by ticker
            order by trade_date
        ) as prior_close
    from {{ ref('fact_daily_bar') }}
)

select
    ticker,
    trade_date,
    close * volume as dollar_volume,
    high - low as range,
    (open - prior_close) / nullif(prior_close, 0) as gap_pct,
    (close - prior_close) / nullif(prior_close, 0) as pct_gain_loss,
    (close - open) / nullif(open, 0) as pct_from_open,
    close - prior_close as dollar_change,
    close - open as dollar_change_from_open,
    (high - open) / nullif(open, 0) as pct_open_to_high,
    (low - open) / nullif(open, 0) as pct_open_to_low
from bars
