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
    handlers=[logging.FileHandler("trades_writer.log"), logging.StreamHandler()]
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
        self.max_retries = 5

    def _touch_heartbeat(self):
        """Creates or updates a heartbeat file to signal liveness."""
        try:
            with open("/tmp/heartbeat", "w") as f:
                f.write("healthy")
        except Exception as e:
            logger.warning(f"Could not write heartbeat file: {e}")
            
    async def connect_to_db(self):
        for attempt in range(self.max_retries):
            try:
                self.db_pool = await asyncpg.create_pool(
                    user=self.db_user, password=self.db_password,
                    database=self.db_name, host=self.db_host,
                    min_size=2, max_size=10
                )
                logger.info("Database connection pool created successfully.")
                return
            except (asyncpg.PostgresError, OSError) as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"DB connection failed (attempt {attempt+1}/{self.max_retries}): {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.critical("FATAL: Could not connect to database after all retries.")
                    raise

    async def connect_to_redis(self):
        for attempt in range(self.max_retries):
            try:
                self.redis_client = redis.from_url(f"redis://{self.redis_host}")
                await self.redis_client.ping()
                logger.info("Connected to Redis successfully.")
                return
            except redis.RedisError as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Redis connection failed (attempt {attempt+1}/{self.max_retries}): {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.critical("FATAL: Could not connect to Redis after all retries.")
                    raise

    async def process_stream(self, stream_name: str, group_name: str, consumer_name: str, handler_func):
        while True:
            try:
                response = await self.redis_client.xreadgroup(
                    group_name, consumer_name, {stream_name: '>'}, count=500, block=2000
                )
                if not response:
                    continue

                stream, messages = response[0]
                records_to_write = []
                message_ids_to_ack = []
                for msg_id, msg_data in messages:
                    try:
                        record = handler_func(json.loads(msg_data[b'data']))
                        if record:
                            records_to_write.append(record)
                        message_ids_to_ack.append(msg_id)
                    except Exception as e:
                        logger.error(f"Skipping malformed message {msg_id} in {stream_name}: {e}")
                        message_ids_to_ack.append(msg_id)

                if records_to_write:
                    success = await self.write_batch_to_db(stream_name, records_to_write)
                    if success:
                        await self.redis_client.xack(stream_name, group_name, *message_ids_to_ack)
                elif message_ids_to_ack:
                    await self.redis_client.xack(stream_name, group_name, *message_ids_to_ack)

            except ResponseError as e:
                if "NOGROUP" in str(e):
                    logger.warning(f"Stream or group for '{stream_name}' missing. Creating...")
                    try:
                        await self.redis_client.xgroup_create(stream_name, group_name, id='0', mkstream=True)
                    except ResponseError as create_e:
                        if "BUSYGROUP" not in str(create_e):
                            logger.error(f"Failed to create group for '{stream_name}': {create_e}")
                            await asyncio.sleep(5)
                else:
                    logger.error(f"Redis error in {stream_name}: {e}", exc_info=True)
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"General error in {stream_name}: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    def handle_trade_message(self, trade_data: Dict[str, Any]) -> Optional[tuple]:
        try:
            return ( float(trade_data['time']), trade_data['symbol'], int(trade_data['trade_id']), str(trade_data['price']), str(trade_data['amount']), bool(trade_data['is_buyer_maker']) )
        except (KeyError, TypeError, ValueError): return None

    async def write_batch_to_db(self, stream_name: str, data: List[tuple]) -> bool:
        if not self.db_pool or not data: return False
        sql_map = {
            "trades_stream": "INSERT INTO trades (time, exchange, symbol, trade_id, price, amount, is_buyer_maker) VALUES (to_timestamp($1), 'binance', $2, $3, $4, $5, $6) ON CONFLICT (time, exchange, symbol, trade_id) DO NOTHING"
        }
        sql = sql_map.get(stream_name)
        if not sql: return False

        try:
            async with self.db_pool.acquire() as conn:
                await conn.executemany(sql, data)
            logger.info(f"Successfully wrote {len(data)} records from {stream_name} to DB.")
            return True
        except (asyncpg.PostgresError, OSError) as e:
            logger.error(f"Database write error for {stream_name}: {e}. Batch will be retried.", exc_info=True)
            return False

    async def run(self):
        await self.connect_to_db()
        await self.connect_to_redis()

        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(self._touch_heartbeat, 'interval', seconds=15)
        scheduler.start()

        await self.process_stream("trades_stream", "trades_group", "trades-writer-1", self.handle_trade_message)

if __name__ == "__main__":
    writer = DatabaseWriter()
    try:
        asyncio.run(writer.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Trades writer service stopped.")