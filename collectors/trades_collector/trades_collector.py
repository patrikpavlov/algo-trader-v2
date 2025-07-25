import asyncio
import logging
import json
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, NamedTuple

import asyncpg
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# --- Logging Setup ---
# Placed at the top for immediate availability.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("trades_collector.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# --- 1. Structured Configuration & Data Types ---
# Using NamedTuples for structured, immutable data. This makes the code
# clearer and less prone to errors than using raw tuples or global constants.

class AppConfig(NamedTuple):
    """Holds all application configuration settings."""
    target_pairs: List[str]
    db_user: Optional[str]
    db_password: Optional[str]
    db_name: Optional[str]
    db_host: Optional[str]
    batch_size: int
    flush_interval_seconds: int
    watchdog_interval_seconds: int
    stale_connection_threshold_seconds: int

class TradeData(NamedTuple):
    """Represents a single trade record for database insertion."""
    time: float
    symbol: str
    trade_id: int
    price: str
    amount: str
    is_buyer_maker: bool


def load_config() -> AppConfig:
    """
    Loads configuration from environment variables, validates types,
    and provides sensible defaults.
    """
    _pairs_str = os.getenv("TARGET_PAIRS", "btcusdt,ethusdt")
    target_pairs = [pair.strip().lower() for pair in _pairs_str.split(',')]

    try:
        batch_size = int(os.getenv("BATCH_SIZE", "200"))
        flush_interval = int(os.getenv("FLUSH_INTERVAL", "5"))
        watchdog_interval = int(os.getenv("WATCHDOG_INTERVAL", "30"))
        stale_threshold = int(os.getenv("STALE_CONNECTION_THRESHOLD", "180"))
    except ValueError as e:
        logger.critical(f"Invalid integer value for an interval in environment variables: {e}")
        raise

    return AppConfig(
        target_pairs=target_pairs,
        db_user=os.getenv("POSTGRES_USER"),
        db_password=os.getenv("POSTGRES_PASSWORD"),
        db_name=os.getenv("POSTGRES_DB"),
        db_host=os.getenv("DB_HOST"),
        batch_size=batch_size,
        flush_interval_seconds=flush_interval,
        watchdog_interval_seconds=watchdog_interval,
        stale_connection_threshold_seconds=stale_threshold,
    )


# --- 2. Encapsulated Application Logic ---
# All logic and state are now contained within a class, eliminating global
# variables and making the system's behavior easier to reason about and test.

class TradeCollector:
    def __init__(self, config: AppConfig):
        self.config = config
        self.db_pool: Optional[asyncpg.Pool] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.shutdown_event = asyncio.Event()

        # Encapsulated state
        self.trade_buffer: List[TradeData] = []
        self.buffer_lock = asyncio.Lock()
        self.last_message_time = time.time()

    def _threadsafe_message_handler(self, msg: str) -> None:
        """
        Thread-safe callback from the WebSocket client.
        Schedules the async handler on the main event loop.
        """
        if self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._process_message(msg), self.loop)

    async def _process_message(self, msg_str: str) -> None:
        """Asynchronously processes a single trade message from the WebSocket."""
        try:
            self.last_message_time = time.time()
            msg = json.loads(msg_str)
            trade_payload = msg.get('data')

            if not trade_payload or trade_payload.get('e') != 'trade':
                return

            trade_data = TradeData(
                time=trade_payload['T'] / 1000.0,  # Convert ms to seconds
                symbol=trade_payload['s'].lower(),
                trade_id=trade_payload['t'],
                price=str(trade_payload['p']),
                amount=str(trade_payload['q']),
                is_buyer_maker=trade_payload['m']
            )

            async with self.buffer_lock:
                self.trade_buffer.append(trade_data)
                buffer_len = len(self.trade_buffer)

            if buffer_len >= self.config.batch_size:
                # Schedule the flush instead of awaiting it to not block message processing
                asyncio.create_task(self._flush_trades_to_db())

        except json.JSONDecodeError:
            logger.error(f"Could not decode JSON from message: {msg_str}")
        except Exception as e:
            logger.error(f"Critical error in message processor: {e}", exc_info=True)

    async def _flush_trades_to_db(self) -> None:
        """Flushes the in-memory trade buffer to the database."""
        async with self.buffer_lock:
            if not self.trade_buffer:
                return
            trades_to_flush = self.trade_buffer.copy()
            self.trade_buffer.clear()

        if not self.db_pool:
            logger.error("Database pool is not available. Cannot flush trades.")
            return

        sql = """
            INSERT INTO trades (time, exchange, symbol, trade_id, price, amount, is_buyer_maker)
            VALUES (to_timestamp($1), 'binance', $2, $3, $4, $5, $6)
            ON CONFLICT (time, exchange, symbol, trade_id) DO NOTHING
        """
        try:
            async with self.db_pool.acquire() as conn:
                await conn.executemany(sql, trades_to_flush)
            logger.info(f"Successfully flushed {len(trades_to_flush)} trades to DB.")
        except (asyncpg.PostgresError, OSError) as e:
            logger.error(f"Database flush error: {e}", exc_info=True)
            # Optional: Re-add trades to buffer for retry, with caution for memory usage
            # async with self.buffer_lock:
            #     self.trade_buffer.extend(trades_to_flush)

    async def _check_stream_health(self) -> None:
        """Watchdog to check for a stale WebSocket connection."""
        time_since_last_msg = time.time() - self.last_message_time
        if time_since_last_msg > self.config.stale_connection_threshold_seconds:
            logger.critical(
                f"Stale connection: No messages received for {time_since_last_msg:.2f}s. "
                f"Threshold is {self.config.stale_connection_threshold_seconds}s. Initiating shutdown."
            )
            self.shutdown_event.set() # Trigger a graceful shutdown and restart

    async def run(self) -> None:
        """The main entry point for running the collector."""
        self.loop = asyncio.get_running_loop()
        self.last_message_time = time.time()

        try:
            self.db_pool = await asyncpg.create_pool(
                user=self.config.db_user,
                password=self.config.db_password,
                database=self.config.db_name,
                host=self.config.db_host,
                min_size=2, max_size=10
            )
            logger.info("Database connection pool created successfully.")
        except (asyncpg.PostgresError, OSError) as e:
            logger.critical(f"FATAL: Could not connect to database: {e}")
            return

        logger.info(f"Starting trade collector for {len(self.config.target_pairs)} pairs: {self.config.target_pairs}")

        client = SpotWebsocketStreamClient(
            on_message=lambda _, msg: self._threadsafe_message_handler(msg),
            is_combined=True
        )
        client.subscribe(stream=[f"{pair}@trade" for pair in self.config.target_pairs])

        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(self._flush_trades_to_db, 'interval', seconds=self.config.flush_interval_seconds)
        scheduler.add_job(self._check_stream_health, 'interval', seconds=self.config.watchdog_interval_seconds)
        scheduler.start()

        logger.info(f"Scheduler started with DB flushes every {self.config.flush_interval_seconds}s.")
        logger.info("Collector is running. Press Ctrl+C to stop.")

        try:
            await self.shutdown_event.wait()
        finally:
            logger.info("Shutdown signal received. Cleaning up...")
            scheduler.shutdown()
            client.stop()
            # Perform one final flush to save any remaining trades
            await self._flush_trades_to_db()
            if self.db_pool:
                await self.db_pool.close()
            logger.info("Collector stopped gracefully.")


# --- 3. Clean Execution Entry Point ---
# The main execution block is now minimal and focuses on setting up
# and running the application class.

if __name__ == "__main__":
    try:
        config = load_config()
        collector = TradeCollector(config)
        asyncio.run(collector.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Application interrupted by user.")
    except Exception as e:
        logger.critical(f"A critical error occurred during startup: {e}", exc_info=True)