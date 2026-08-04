-- Staging: thin pass-through over the silver Parquet source, normalizing
-- column names/types once so every downstream model agrees on the shape.
select
    symbol,
    sequence,
    price,
    size,
    side,
    exchange,
    channel,
    exchange_timestamp,
    ingest_timestamp,
    cast(exchange_timestamp as date) as tick_date,
    date_part('hour', exchange_timestamp) as tick_hour
from {{ source('dataeng_silver', 'ticks') }}
