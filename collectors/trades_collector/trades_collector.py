import asyncio
import logging
import json
import os
import time
from typing import List, Optional, NamedTuple

import asyncpg
import redis.asyncio as redis
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("trades_collector.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# --- Configuration ---
class AppConfig(NamedTuple):
    """Holds all application configuration settings."""
    redis_host: str
    db_user: Optional[str]
    db_password: Optional[str]
    db_name: Optional[str]
    db_host: Optional[str]
    batch_size: int
    flush_interval_seconds: int
    watchdog_interval_seconds: int
    stale_connection_threshold_seconds: int
    max_stream_len: int

def load_config() -> AppConfig:
    """Loads configuration from environment variables and provides sensible defaults."""
    return AppConfig(
        redis_host=os.getenv("REDIS_HOST", "redis"),
        db_user=os.getenv("POSTGRES_USER"),
        db_password=os.getenv("POSTGRES_PASSWORD"),
        db_name=os.getenv("POSTGRES_DB"),
        db_host=os.getenv("DB_HOST", "db"),
        batch_size=int(os.getenv("BATCH_SIZE", "200")),
        flush_interval_seconds=int(os.getenv("FLUSH_INTERVAL", "5")),
        watchdog_interval_seconds=int(os.getenv("WATCHDOG_INTERVAL", "30")),
        stale_connection_threshold_seconds=int(os.getenv("STALE_CONNECTION_THRESHOLD", "180")),
        max_stream_len=int(os.getenv("MAX_STREAM_LEN", "1000000")),
    )


# --- Application Logic ---
class TradeCollector:
    def __init__(self, config: AppConfig):
        self.config = config
        self.redis_client: Optional[redis.Redis] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.shutdown_event = asyncio.Event()

        # Encapsulated state
        self.trade_buffer: list = []
        self.buffer_lock = asyncio.Lock()
        self.last_message_time = time.time()

    async def _fetch_active_symbols(self) -> List[str]:
        """Fetches the list of active symbols from the database."""
        conn = None
        try:
            conn = await asyncpg.connect(
                user=self.config.db_user, password=self.config.db_password,
                database=self.config.db_name, host=self.config.db_host
            )
            records = await conn.fetch("SELECT symbol FROM monitored_symbols WHERE is_active = true")
            symbols = [rec['symbol'] for rec in records]
            logger.info(f"Loaded {len(symbols)} active symbols from database.")
            return symbols
        except Exception as e:
            logger.critical(f"FATAL: Could not fetch symbols from database: {e}")
            return []
        finally:
            if conn:
                await conn.close()

    def _threadsafe_message_handler(self, msg: str) -> None:
        """Thread-safe callback that schedules the async handler on the main event loop."""
        if self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._process_message(msg), self.loop)

    async def _process_message(self, msg_str: str) -> None:
        """Asynchronously processes a single trade message."""
        try:
            self.last_message_time = time.time()
            msg = json.loads(msg_str)
            trade_payload = msg.get('data')

            if not trade_payload or trade_payload.get('e') != 'trade':
                return

            trade_data = {
                "time": trade_payload['T'] / 1000.0,
                "symbol": trade_payload['s'].lower(),
                "trade_id": trade_payload['t'],
                "price": str(trade_payload['p']),
                "amount": str(trade_payload['q']),
                "is_buyer_maker": trade_payload['m']
            }

            async with self.buffer_lock:
                self.trade_buffer.append(trade_data)
                if len(self.trade_buffer) >= self.config.batch_size:
                    asyncio.create_task(self._flush_trades_to_redis())

        except json.JSONDecodeError:
            logger.error(f"Could not decode JSON from message: {msg_str}")
        except Exception as e:
            logger.error(f"Critical error in message processor: {e}", exc_info=True)

    async def _flush_trades_to_redis(self) -> None:
        """Flushes the in-memory trade buffer to the Redis Stream with retries and a capped size."""
        async with self.buffer_lock:
            if not self.trade_buffer:
                return
            trades_to_flush = self.trade_buffer
            self.trade_buffer = []

        if not self.redis_client:
            logger.error("Redis client not available. Re-buffering trades.")
            async with self.buffer_lock:
                self.trade_buffer.extend(trades_to_flush)
            return

        for attempt in range(5):
            try:
                pipe = self.redis_client.pipeline()
                for trade in trades_to_flush:
                    pipe.xadd("trades_stream", {"data": json.dumps(trade)}, maxlen=self.config.max_stream_len, approximate=True)
                await pipe.execute()
                logger.info(f"Successfully flushed {len(trades_to_flush)} trades to Redis Stream.")
                return

            except redis.RedisError as e:
                logger.warning(f"Redis flush attempt {attempt+1} failed: {e}")
                if attempt == 4:
                    logger.critical("All Redis flush attempts failed. Re-buffering trades to prevent data loss.")
                    async with self.buffer_lock:
                        self.trade_buffer.extend(trades_to_flush)
                    break
                await asyncio.sleep(1 * (2 ** attempt))

    async def _check_stream_health(self) -> None:
        """Watchdog to check for a stale WebSocket connection."""
        time_since_last_msg = time.time() - self.last_message_time
        if time_since_last_msg > self.config.stale_connection_threshold_seconds:
            logger.critical(
                f"Stale connection: No messages received for {time_since_last_msg:.2f}s. "
                f"Threshold is {self.config.stale_connection_threshold_seconds}s. Initiating shutdown."
            )
            self.shutdown_event.set()

    async def run(self) -> None:
        """The main entry point for running the collector."""
        self.loop = asyncio.get_running_loop()
        self.last_message_time = time.time()

        target_pairs = await self._fetch_active_symbols()
        if not target_pairs:
            logger.critical("No active symbols to monitor. Shutting down.")
            return

        try:
            self.redis_client = redis.from_url(f"redis://{self.config.redis_host}")
            await self.redis_client.ping()
            logger.info("Connected to Redis successfully.")
        except redis.RedisError as e:
            logger.critical(f"FATAL: Could not connect to Redis: {e}")
            return

        logger.info(f"Starting trade collector for {len(target_pairs)} pairs: {target_pairs}")

        client = SpotWebsocketStreamClient(
            on_message=lambda _, msg: self._threadsafe_message_handler(msg),
            is_combined=True
        )
        client.subscribe(stream=[f"{pair}@trade" for pair in target_pairs])

        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(self._flush_trades_to_redis, 'interval', seconds=self.config.flush_interval_seconds, id="redis_flush")
        scheduler.add_job(self._check_stream_health, 'interval', seconds=self.config.watchdog_interval_seconds, id="health_check")
        scheduler.start()

        logger.info(f"Scheduler started with Redis flushes every {self.config.flush_interval_seconds}s.")
        logger.info("Collector is running. Press Ctrl+C to stop.")

        try:
            await self.shutdown_event.wait()
        finally:
            logger.info("Shutdown signal received. Cleaning up...")
            scheduler.shutdown()
            client.stop()
            await self._flush_trades_to_redis()
            if self.redis_client:
                await self.redis_client.close()
            logger.info("Collector stopped gracefully.")


# --- Execution Entry Point ---
if __name__ == "__main__":
    try:
        config = load_config()
        collector = TradeCollector(config)
        asyncio.run(collector.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Application interrupted by user.")
    except Exception as e:
        logger.critical(f"A critical error occurred during startup: {e}", exc_info=True)