import asyncio
import logging
import os
import json
from typing import Optional, List, Dict, Any

import asyncpg
import redis.asyncio as redis
from redis.exceptions import ResponseError

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("writer.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class DatabaseWriter:
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis_client: Optional[redis.Redis] = None
        self.db_user = os.getenv("POSTGRES_USER")
        self.db_password = os.getenv("POSTGRES_PASSWORD")
        self.db_name = os.getenv("POSTGRES_DB")
        self.db_host = os.getenv("DB_HOST", "db")
        self.redis_host = os.getenv("REDIS_HOST", "redis")

    async def connect_to_db(self):
        try:
            self.db_pool = await asyncpg.create_pool(
                user=self.db_user, password=self.db_password,
                database=self.db_name, host=self.db_host,
                min_size=2, max_size=10
            )
            logger.info("Database connection pool created successfully.")
        except (asyncpg.PostgresError, OSError) as e:
            logger.critical(f"FATAL: Could not connect to database: {e}")
            raise

    async def connect_to_redis(self):
        try:
            self.redis_client = redis.from_url(f"redis://{self.redis_host}")
            await self.redis_client.ping()
            logger.info("Connected to Redis successfully.")
        except redis.RedisError as e:
            logger.critical(f"FATAL: Could not connect to Redis: {e}")
            raise

    async def process_stream(self, stream_name: str, group_name: str, consumer_name: str, handler_func):
        try:
            await self.redis_client.xgroup_create(stream_name, group_name, id='0', mkstream=True)
            logger.info(f"Consumer group '{group_name}' created or already exists for stream '{stream_name}'.")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{group_name}' already exists for stream '{stream_name}'.")
            else:
                logger.error(f"Error creating consumer group for {stream_name}: {e}")
                return

        while True:
            try:
                response = await self.redis_client.xreadgroup(
                    group_name, consumer_name, {stream_name: '>'}, count=100, block=2000
                )
                if not response:
                    continue

                stream, messages = response[0]
                records_to_write = [handler_func(json.loads(msg[b'data'])) for _, msg in messages]
                message_ids_to_ack = [msg_id for msg_id, _ in messages]

                if records_to_write:
                    await self.write_batch_to_db(stream_name, records_to_write)
                    await self.redis_client.xack(stream_name, group_name, *message_ids_to_ack)
            except Exception as e:
                logger.error(f"Error processing stream {stream_name}: {e}", exc_info=True)
                await asyncio.sleep(1)

    def handle_trade_message(self, trade_data: Dict[str, Any]):
        return (
            float(trade_data['time']), trade_data['symbol'], int(trade_data['trade_id']),
            str(trade_data['price']), str(trade_data['amount']), bool(trade_data['is_buyer_maker'])
        )

    def handle_snapshot_message(self, snapshot_data: Dict[str, Any]):
         return (
            snapshot_data['time'], 'binance', snapshot_data['symbol'],
            snapshot_data['bids'], snapshot_data['asks']
        )

    async def write_batch_to_db(self, stream_name: str, data: List[tuple]):
        if not self.db_pool or not data:
            return

        sql_map = {
            "trades_stream": """
                INSERT INTO trades (time, exchange, symbol, trade_id, price, amount, is_buyer_maker)
                VALUES (to_timestamp($1), 'binance', $2, $3, $4, $5, $6)
                ON CONFLICT (time, exchange, symbol, trade_id) DO NOTHING
            """,
            "snapshots_stream": """
                INSERT INTO orderbook_snapshots (time, exchange, symbol, bids, asks)
                VALUES (to_timestamp($1), $2, $3, $4, $5)
            """
        }
        sql = sql_map.get(stream_name)
        if not sql:
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.executemany(sql, data)
            logger.info(f"Successfully wrote {len(data)} records from {stream_name} to DB.")
        except (asyncpg.PostgresError, OSError) as e:
            logger.error(f"Database write error for {stream_name}: {e}", exc_info=True)
            raise

    async def run(self):
        await self.connect_to_db()
        await self.connect_to_redis()
        await asyncio.gather(
            self.process_stream("trades_stream", "trades_group", "writer-1", self.handle_trade_message),
            self.process_stream("snapshots_stream", "snapshots_group", "writer-1", self.handle_snapshot_message)
        )

if __name__ == "__main__":
    writer = DatabaseWriter()
    try:
        asyncio.run(writer.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Writer service stopped.")