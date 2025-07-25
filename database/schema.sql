CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE trades (
    time TIMESTAMPTZ NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    trade_id BIGINT NOT NULL,
    price NUMERIC NOT NULL,
    amount NUMERIC NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL
);
-- Add "time" to the primary key
ALTER TABLE trades ADD PRIMARY KEY (time, exchange, symbol, trade_id);
SELECT create_hypertable('trades', 'time');

CREATE TABLE orderbook_snapshots (
    time TIMESTAMPTZ NOT NULL,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bids JSONB,
    asks JSONB
);
-- Add "time" to the unique index
CREATE UNIQUE INDEX idx_snapshots_unique ON orderbook_snapshots (time, exchange, symbol);
SELECT create_hypertable('orderbook_snapshots', 'time');

-- Symbol monitoring table
CREATE TABLE monitored_symbols (
    symbol TEXT PRIMARY KEY,
    is_active BOOLEAN NOT NULL DEFAULT true,
    date_added TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO monitored_symbols (symbol) VALUES
('btcusdt'), ('ethusdt'), ('xrpusdt'), ('bnbusdt'), ('solusdt'),
('dogeusdt'), ('adausdt'), ('trxusdt'), ('xlmusdt'), ('suiusdt'),
('linkusdt'), ('hbarusdt'), ('bchusdt'), ('avaxusdt'), ('ltcusdt'),
('shibusdt'), ('dotusdt'), ('uniusdt'), ('xmrusdt'), ('pepeusdt'),
('aaveusdt'), ('crousdt'), ('nearusdt'), ('etcusdt'), ('aptusdt'),
('ondousdt'), ('icpusdt'), ('kasusdt'), ('mntusdt'), ('algousdt'),
('arbusdt'), ('vetusdt'), ('atomusdt'), ('renderusdt'), ('wldusdt'),
('fetusdt'), ('seiusdt'), ('filusdt'), ('qntusdt'), ('jupusdt'),
('flrusdt'), ('tiausdt'), ('flokiusdt'), ('injusdt'), ('nexousdt'),
('crvusdt'), ('stxusdt');
