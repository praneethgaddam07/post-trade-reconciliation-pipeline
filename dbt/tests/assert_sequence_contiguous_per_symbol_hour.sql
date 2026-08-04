-- Singular test: sequence numbers must be contiguous per symbol per hour.
-- A returned row means a gap exists — the SQL-layer counterpart to the
-- streaming reconciler's sequence_gap break type (posttrade/reconcile),
-- expressed here as a dbt data test instead of a pandas check. A passing
-- test (zero rows) means every observed sequence run in that partition is
-- unbroken.
with numbered as (
    select
        symbol,
        tick_date,
        tick_hour,
        sequence,
        lag(sequence) over (
            partition by symbol, tick_date, tick_hour
            order by sequence
        ) as prev_sequence
    from {{ ref('fct_ticks') }}
)
select
    symbol,
    tick_date,
    tick_hour,
    prev_sequence,
    sequence,
    sequence - prev_sequence - 1 as missing_count
from numbered
where prev_sequence is not null
  and sequence - prev_sequence > 1
