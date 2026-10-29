-- Calendar spine across loaded staging trade_date range.
-- Holiday = weekday with no staging bars that day.
-- Extract already uses XNYS via exchange_calendars in Python; seeding that
-- calendar into Snowflake would be a new object. Keep the weekday-empty rule.
-- date_spine end_date is exclusive in practice (datediff), so pass max + 1 day.

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(select min(trade_date) from " ~ ref('stg_daily_bars') ~ ")",
        end_date="(select dateadd(day, 1, max(trade_date)) from " ~ ref('stg_daily_bars') ~ ")"
    ) }}
),

days_with_bars as (
    select distinct trade_date
    from {{ ref('stg_daily_bars') }}
)

select
    spine.date_day,
    -- dayofweekiso: Monday = 1 .. Sunday = 7 (locale-stable vs dayname).
    dayofweekiso(spine.date_day) in (6, 7) as is_weekend,
    dayofweekiso(spine.date_day) not in (6, 7)
        and bars.trade_date is null as is_holiday
from date_spine as spine
left join days_with_bars as bars
    on spine.date_day = bars.trade_date
