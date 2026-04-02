with base as (
    select * from {{ ref('stg_prices') }}
),

windowed as (
    select
        ticker,
        date,
        open,
        high,
        low,
        close,
        volume,

        avg(close) over w7   as sma_7,
        avg(close) over w21  as sma_21,
        avg(close) over w50  as sma_50,
        avg(close) over w12  as ema_12,
        avg(close) over w26  as ema_26,

        (close - lag(close) over (partition by ticker order by date))
            / nullif(lag(close) over (partition by ticker order by date), 0)
            as daily_return,

        stddev(close) over w30 * sqrt(252) as volatility_30d,

        greatest(
            high - low,
            abs(high - lag(close) over (partition by ticker order by date)),
            abs(low  - lag(close) over (partition by ticker order by date))
        ) as true_range

    from base

    window
        w7  as (partition by ticker order by date rows between 6  preceding and current row),
        w12 as (partition by ticker order by date rows between 11 preceding and current row),
        w21 as (partition by ticker order by date rows between 20 preceding and current row),
        w26 as (partition by ticker order by date rows between 25 preceding and current row),
        w30 as (partition by ticker order by date rows between 29 preceding and current row),
        w50 as (partition by ticker order by date rows between 49 preceding and current row)
)

select
    *,
    ema_12 - ema_26 as macd,
    avg(true_range) over (
        partition by ticker order by date
        rows between 13 preceding and current row
    ) as atr_14

from windowed
