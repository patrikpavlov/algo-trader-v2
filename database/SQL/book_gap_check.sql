WITH symbol_time_range AS (
  SELECT
    symbol,
    date_trunc('second', MIN(time)) AS min_time,
    date_trunc('second', MAX(time)) AS max_time
  FROM
    crypto_data.public.orderbook_snapshots
  GROUP BY
    symbol
),
complete_timeline AS (
  SELECT
    r.symbol,
    s.sec AS expected_time
  FROM
    symbol_time_range r,
    generate_series(r.min_time, r.max_time, '1 second'::interval) AS s(sec)
),
actual_snapshots AS (
  SELECT DISTINCT
    symbol,
    date_trunc('second', time) AS actual_time
  FROM
    crypto_data.public.orderbook_snapshots
)
SELECT
  t.symbol,
  t.expected_time AS missing_snapshot_second
FROM
  complete_timeline t
  LEFT JOIN actual_snapshots a
    ON t.symbol = a.symbol AND t.expected_time = a.actual_time
WHERE
  a.actual_time IS NULL
ORDER BY
  t.symbol,
  t.expected_time;