WITH ranked_trades AS (
  SELECT
    symbol,
    trade_id,
    LAG(trade_id, 1) OVER (PARTITION BY symbol ORDER BY trade_id ASC) AS prev_trade_id
  FROM
    crypto_data.public.trades
)
SELECT
  symbol,
  prev_trade_id,
  trade_id AS first_trade_after_gap,
  (trade_id - prev_trade_id - 1) AS num_trades_missed
FROM
  ranked_trades
WHERE
  trade_id != (prev_trade_id + 1)
  AND prev_trade_id IS NOT NULL
ORDER BY
  symbol,
  prev_trade_id;