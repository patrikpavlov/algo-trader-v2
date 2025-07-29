import asyncio
import logging
import os
import json
import socket
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import time # Import time for measuring durations

import asyncpg
import redis.asyncio as redis
from redis.exceptions import ResponseError, ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# --- Logging Setup ---
# Set to INFO for production. Change to DEBUG for deep diagnostics.
logging.basicConfig(
    level=logging.INFO, # Keep at DEBUG for detailed insights
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[logging.FileHandler("trades_writer.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class DatabaseWriter:
    """
    A robust service to consume trade data from a Redis stream and write it to a
    PostgreSQL database. It is designed to be self-healing and resilient to
    network interruptions for both Redis and PostgreSQL.
    """
    def __init__(self):
        logger.debug("Initializing DatabaseWriter instance.")
        # Connection clients
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis_client: Optional[redis.Redis] = None

        # Configuration
        self.db_user = os.getenv("POSTGRES_USER")
        self.db_password = os.getenv("POSTGRES_PASSWORD")
        self.db_name = os.getenv("POSTGRES_DB")
        self.db_host = os.getenv("DB_HOST", "db")
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        
        # State Management for resilient connections
        self.redis_connected = asyncio.Event()
        self.db_connected = asyncio.Event()
        self.reconnecting_redis = asyncio.Lock()
        self.reconnecting_db = asyncio.Lock()
        
        self.last_message_time = None
        logger.debug("DatabaseWriter instance initialized.")

    # --- Connection Management with Exponential Backoff ---

    def _touch_heartbeat(self):
        """Creates or updates a heartbeat file to signal liveness."""
        try:
            with open("/tmp/heartbeat", "w") as f:
                f.write("healthy")
        except Exception as e:
            logger.warning(f"Could not write heartbeat file: {e}")

    async def _manage_redis_connection(self):
        """A background task to establish and maintain the Redis connection."""
        async with self.reconnecting_redis:
            backoff_delay = 1
            while not self.redis_connected.is_set():
                try:
                    logger.info("Attempting to connect to Redis...")
                    if self.redis_client:
                        await self.redis_client.close()
                    
                    self.redis_client = redis.from_url(f"redis://{self.redis_host}",
                                                       socket_connect_timeout=5,
                                                       socket_keepalive=True)
                    await self.redis_client.ping()
                    
                    logger.info("Successfully connected to Redis.")
                    self.redis_connected.set()
                except (RedisConnectionError, RedisTimeoutError, OSError) as e:
                    logger.error(f"Redis connection failed: {e}. Retrying in {backoff_delay}s...")
                    self.redis_connected.clear()
                    await asyncio.sleep(backoff_delay)
                    backoff_delay = min(backoff_delay * 2, 60)

    async def _manage_db_connection(self):
        """A background task to establish and maintain the Database connection pool."""
        async with self.reconnecting_db:
            backoff_delay = 1
            while not self.db_connected.is_set():
                try:
                    logger.info("Attempting to connect to database...")
                    if self.db_pool:
                        await self.db_pool.close()

                    self.db_pool = await asyncpg.create_pool(
                        user=self.db_user, password=self.db_password,
                        database=self.db_name, host=self.db_host,
                        min_size=2, max_size=10, timeout=10, command_timeout=30
                    )
                    
                    logger.info("Successfully connected to Database.")
                    self.db_connected.set()
                except (asyncpg.PostgresError, OSError, socket.gaierror) as e:
                    logger.error(f"Database connection failed: {e}. Retrying in {backoff_delay}s...")
                    self.db_connected.clear()
                    await asyncio.sleep(backoff_delay)
                    backoff_delay = min(backoff_delay * 2, 60)
    
    # --- Proactive Health Checks ---

    async def _health_check(self):
        """Periodically checks the health of dependencies."""
        logger.debug("Starting periodic health check.")
        
        # Proactively check Redis with a ping
        if self.redis_connected.is_set():
            try:
                await asyncio.wait_for(self.redis_client.ping(), timeout=5)
            except Exception as e:
                logger.warning(f"Health check failed for Redis: {e}. Triggering reconnect.")
                self.redis_connected.clear()
                if not self.reconnecting_redis.locked():
                    asyncio.create_task(self._manage_redis_connection())
        
        # Non-invasively check DB pool status. Real failures are caught during writes.
        if self.db_connected.is_set() and self.db_pool.is_closing():
            logger.warning("Health check found DB pool is closing. Triggering reconnect.")
            self.db_connected.clear()
            if not self.reconnecting_db.locked():
                asyncio.create_task(self._manage_db_connection())
        
        status = f"HEALTH CHECK STATUS: Redis OK: {self.redis_connected.is_set()}, DB OK: {self.db_connected.is_set()}"
        logger.info(status)

    # --- Core Application Logic ---

    async def process_stream(self, stream_name: str, group_name: str, consumer_name: str, handler_func):
        while True:
            try:
                await self.redis_connected.wait()
                await self.db_connected.wait()

                # --- Debugging: Measure Redis read time ---
                start_read_time = time.time()
                response = await asyncio.wait_for(
                    self.redis_client.xreadgroup(
                        group_name, consumer_name, {stream_name: '>'}, count=5000, block=5000
                    ),
                    timeout=10.0
                )
                read_duration = time.time() - start_read_time

                if not response:
                    logger.debug(f"[READ] No new messages after {read_duration:.2f}s block.")
                    continue

                self.last_message_time = datetime.now(timezone.utc)
                stream, messages = response[0]
                num_messages_received = len(messages)
                logger.info(f"[READ] Received {num_messages_received} messages from Redis in {read_duration:.2f}s.")
                
                records_to_write = [rec for msg_id, msg_data in messages if (rec := handler_func(msg_data)) is not None]
                message_ids_to_ack = [msg_id for msg_id, _ in messages]

                if records_to_write:
                    # --- Debugging: Measure DB write time ---
                    start_write_time = time.time()
                    write_success = await self.write_batch_to_db(records_to_write)
                    write_duration = time.time() - start_write_time

                    if write_success:
                        logger.info(f"[WRITE] Successfully wrote {len(records_to_write)} records to DB in {write_duration:.2f}s.")
                    else:
                        logger.warning(f"[WRITE] Failed to write batch of {len(records_to_write)} records to DB in {write_duration:.2f}s. Messages will be re-processed by consumer group.")
                        message_ids_to_ack = [] # Clear acks for this failed batch
                else:
                    logger.debug("[WRITE] No valid records to write after parsing.")
                
                if message_ids_to_ack:
                    # --- Debugging: Measure Redis ack time ---
                    start_ack_time = time.time()
                    await self.redis_client.xack(stream_name, group_name, *message_ids_to_ack)
                    ack_duration = time.time() - start_ack_time
                    logger.info(f"[ACK] Acknowledged {len(message_ids_to_ack)} messages in Redis in {ack_duration:.2f}s.")

            except asyncio.TimeoutError:
                logger.warning("[ERROR] Redis read call timed out. Connection is likely stale or no data. Triggering reconnect.")
                self.redis_connected.clear()
                if not self.reconnecting_redis.locked():
                    asyncio.create_task(self._manage_redis_connection())
            
            except (RedisConnectionError, RedisTimeoutError) as e:
                logger.warning(f"[ERROR] Redis connection error during operation: {e}. Triggering reconnect.")
                self.redis_connected.clear()
                if not self.reconnecting_redis.locked():
                    asyncio.create_task(self._manage_redis_connection())

            except ResponseError as e:
                if "NOGROUP" in str(e):
                    logger.warning(f"[ERROR] Consumer group '{group_name}' not found. Creating...")
                    try:
                        await self.redis_client.xgroup_create(stream_name, group_name, id='0', mkstream=True)
                    except Exception as create_e:
                        logger.error(f"[ERROR] Failed to create group: {create_e}")
                        await asyncio.sleep(5)
                else:
                    logger.error(f"[ERROR] Unhandled Redis ResponseError: {e}", exc_info=True)
                    await asyncio.sleep(5)
            
            except Exception as e:
                logger.critical(f"[CRITICAL] An unexpected critical error occurred in the main processing loop: {e}", exc_info=True)
                await asyncio.sleep(10)

    def handle_trade_message(self, msg_data: Dict[bytes, bytes]) -> Optional[tuple]:
        """Parses a trade message, returning a tuple for DB insertion or None if invalid."""
        try:
            trade_data = json.loads(msg_data[b'data'])
            return (
                float(trade_data['time']),
                trade_data['symbol'],
                int(trade_data['trade_id']),
                str(trade_data['price']),
                str(trade_data['amount']),
                bool(trade_data['is_buyer_maker'])
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            logger.warning(f"Skipping malformed message due to {e}. Data: {msg_data!r}")
            return None

    async def write_batch_to_db(self, data: List[tuple]) -> bool:
        """Writes a batch of records to the database. Returns True on success, False on failure."""
        if not data: return True
        sql = """
            INSERT INTO trades (time, exchange, symbol, trade_id, price, amount, is_buyer_maker)
            VALUES (to_timestamp($1), 'binance', $2, $3, $4, $5, $6)
            ON CONFLICT (time, exchange, symbol, trade_id) DO NOTHING
        """
        try:
            async with self.db_pool.acquire() as conn:
                await conn.executemany(sql, data)
            # Log message is now handled in process_stream for consolidated timing
            return True
        except (asyncpg.PostgresError, OSError, asyncio.TimeoutError) as e:
            logger.error(f"[DATABASE_ERROR] DATABASE WRITE FAILED: {e}. Triggering reconnect.", exc_info=True)
            self.db_connected.clear()
            if not self.reconnecting_db.locked():
                asyncio.create_task(self._manage_db_connection())
            return False

    async def run(self):
        """Main entry point to set up connections and start the processing loop."""
        logger.info("--- Starting Database Writer Service ---")
        
        # Initial connection attempts
        await asyncio.gather(
            self._manage_db_connection(),
            self._manage_redis_connection()
        )

        # Setup periodic health checks
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(self._health_check, 'interval', seconds=30, id="health_check")
        scheduler.add_job(self._touch_heartbeat, 'interval', seconds=30)
        scheduler.start()

        consumer_name = f"trades-writer-{socket.gethostname()}-{os.getpid()}"
        logger.info(f"Starting consumer with unique name: {consumer_name}")

        try:
            await self.process_stream("trades_stream", "trades_group", consumer_name, self.handle_trade_message)
        finally:
            logger.warning("--- Shutting Down Database Writer Service ---")
            scheduler.shutdown()
            if self.redis_client: await self.redis_client.close()
            if self.db_pool: await self.db_pool.close()

if __name__ == "__main__":
    writer = DatabaseWriter()
    try:
        asyncio.run(writer.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Trades writer service stopped by user.")
    except Exception as e:
        logger.critical(f"Trades writer service exited due to an unhandled exception: {e}", exc_info=True)