select
    symbol || '-' || sequence as tick_id,
    symbol,
    sequence,
    price,
    size,
    side,
    channel,
    exchange_timestamp,
    ingest_timestamp,
    tick_date,
    tick_hour,
    datediff('microsecond', exchange_timestamp, ingest_timestamp) as ingest_latency_us
from {{ ref('stg_ticks') }}
