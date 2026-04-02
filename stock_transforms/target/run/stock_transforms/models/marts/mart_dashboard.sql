
  
    
    

    create  table
      "stock"."main"."mart_dashboard__dbt_tmp"
  
    as (
      select
    ticker,
    date,
    close,
    volume,
    sma_7,
    sma_21,
    sma_50,
    macd,
    round(volatility_30d * 100, 2)  as volatility_pct,
    round(daily_return   * 100, 4)  as daily_return_pct,
    atr_14,

    case
        when sma_7 > sma_21 then 'bullish'
        when sma_7 < sma_21 then 'bearish'
        else 'neutral'
    end as signal

from "stock"."main"."int_moving_averages"
order by ticker, date
    );
  
  