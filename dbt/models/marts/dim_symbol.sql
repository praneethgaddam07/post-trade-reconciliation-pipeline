select distinct
    symbol,
    exchange
from {{ ref('stg_ticks') }}
