with enriched as (
   select
    symbol,
    fetched_at,
    day_low,
    day_high,
    current_price
from {{ ref('silver_clean_stock_quotes') }}
),

candles as (
    select
        symbol,
        fetched_at as candle_time,
        day_low as candle_low,
        day_high as candle_high,
        current_price as candle_open,
        current_price as candle_close,
        current_price as trend_line
    from enriched
),

ranked as (
    select
        c.*,
        row_number() over (
            partition by symbol
            order by candle_time desc
        ) as rn
    from candles c
)

select
    symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
from ranked
where rn <= 12
order by symbol, candle_time