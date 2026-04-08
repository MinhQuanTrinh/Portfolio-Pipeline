{{ config(materialized='table') }}

with source as (
    select
        ticker,
        cast(date as TIMESTAMP_NS)        as date,
        cast(open as double)      as open,
        cast(high as double)      as high,
        cast(low as double)       as low,
        cast(close as double)     as close,
        cast(volume as bigint)    as volume,
        cast(ingested_at as varchar) as ingested_at
    from read_parquet(
        's3://stock-raw/*/year=*/month=*/*.parquet',
        hive_partitioning = true
    )
    where close is not null
      and volume > 0
),

deduped as (
    select *,
        row_number() over (
            partition by ticker, date
            order by ingested_at desc
        ) as rn
    from source
)

select
    ticker,
    date,
    open,
    high,
    low,
    close,
    volume,
    ingested_at
from deduped
where rn = 1