with source as (
    select *
    from read_parquet(
        's3://stock-raw/*/year=*/month=*/*.parquet',
        hive_partitioning = true
    )
)

select
    ticker,
    cast(date as date)     as date,
    cast(open as double)   as open,
    cast(high as double)   as high,
    cast(low as double)    as low,
    cast(close as double)  as close,
    cast(volume as bigint) as volume,
    ingested_at

from source

where close is not null
  and volume > 0
