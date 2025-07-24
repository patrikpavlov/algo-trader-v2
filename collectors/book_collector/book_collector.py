import asyncio
import logging
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple, NamedTuple

import asyncpg
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("book_collector.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# --- 1. Structured Configuration (Standard Library only) ---
# A simple data class to hold settings, providing structure without new dependencies.
class AppConfig(NamedTuple):
    target_pairs: List[str]
    db_user: Optional[str]
    db_password: Optional[str]
    db_name: Optional[str]
    db_host: Optional[str]
    snapshot_interval_seconds: int
    watchdog_interval_seconds: int
    stale_connection_threshold_seconds: int
    binance_api_url: str

def load_config() -> AppConfig:
    """Loads configuration from environment variables and provides defaults."""
    _pairs_str = os.getenv("TARGET_PAIRS", "btcusdt,ethusdt")
    target_pairs = [pair.strip().lower() for pair in _pairs_str.split(',')]

    try:
        snapshot_interval = int(os.getenv("SNAPSHOT_INTERVAL", "1"))
        watchdog_interval = int(os.getenv("WATCHDOG_INTERVAL", "15"))
        stale_threshold = int(os.getenv("STALE_CONNECTION_THRESHOLD", "60"))
    except ValueError as e:
        logger.critical(f"Invalid integer value for an interval in environment variables: {e}")
        raise

    return AppConfig(
        target_pairs=target_pairs,
        db_user=os.getenv("POSTGRES_USER"),
        db_password=os.getenv("POSTGRES_PASSWORD"),
        db_name=os.getenv("POSTGRES_DB"),
        db_host=os.getenv("DB_HOST"),
        snapshot_interval_seconds=snapshot_interval,
        watchdog_interval_seconds=watchdog_interval,
        stale_connection_threshold_seconds=stale_threshold,
        binance_api_url="https://api.binance.com/api/v3",
    )


# --- 2. Encapsulation in a Class ---
# All logic and state are now contained here, eliminating global variables.
class OrderBookCollector:
    def __init__(self, config: AppConfig):
        self.config = config
        self.db_pool: Optional[asyncpg.Pool] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.shutdown_event = asyncio.Event()

        # Initialize state for each trading pair
        self.order_books: Dict[str, Dict[str, Any]] = {
            pair: {"bids": {}, "asks": {}, "lastUpdateId": None, "lock": asyncio.Lock(), "status": "UNINITIALIZED", "buffer": []}
            for pair in self.config.target_pairs
        }
        self.last_message_times: Dict[str, float] = {pair: 0 for pair in self.config.target_pairs}

    def _threadsafe_message_handler(self, msg: str) -> None:
        """Callback from WebSocket thread. Schedules the async handler on the main event loop."""
        if self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._async_handle_message(msg), self.loop)

    async def _async_handle_message(self, msg_str: str) -> None:
        """Asynchronously processes a message from the WebSocket."""
        try:
            msg = json.loads(msg_str)
            diff_payload = msg.get('data', {})
            if diff_payload.get('e') != 'depthUpdate':
                return

            pair = diff_payload['s'].lower()
            if pair not in self.order_books:
                return

            self.last_message_times[pair] = time.time()
            book_state = self.order_books[pair]

            if book_state['status'] in ['UNINITIALIZED', 'SYNCING', 'RESYNCING']:
                async with book_state['lock']:
                    book_state['buffer'].append(diff_payload)
            elif book_state['status'] == 'SYNCED':
                await self._apply_diff(diff_payload)

        except json.JSONDecodeError:
            logger.error(f"Could not decode JSON from message: {msg_str}")
        except Exception as e:
            logger.error(f"!!! CRITICAL ERROR in message handler: {e}", exc_info=True)

    # --- 3. Consolidated Diff Logic ---
    async def _apply_diff_to_book(self, diff: Dict[str, Any], book_state: Dict[str, Any]):
        """Applies a single differential update to a given book state. Assumes lock is held."""
        for price, qty in diff['b']:
            if Decimal(qty) == Decimal('0'):
                book_state['bids'].pop(price, None)
            else:
                book_state['bids'][price] = qty
        for price, qty in diff['a']:
            if Decimal(qty) == Decimal('0'):
                book_state['asks'].pop(price, None)
            else:
                book_state['asks'][price] = qty
        book_state['lastUpdateId'] = diff['u']

    async def _apply_diff(self, diff: Dict[str, Any]) -> None:
        """Validates and applies a differential update from the stream."""
        pair = diff['s'].lower()
        book_state = self.order_books[pair]
        async with book_state['lock']:
            if book_state['status'] != 'SYNCED' or diff['u'] <= book_state['lastUpdateId']:
                return

            if diff['U'] > book_state['lastUpdateId'] + 1:
                logger.warning(f"Gap detected for {pair}. Triggering re-sync.")
                asyncio.create_task(self._resync_book(pair))
                return

            await self._apply_diff_to_book(diff, book_state)

    async def _resync_book(self, pair: str) -> None:
        """Initiates a re-synchronization for a specific pair."""
        book_state = self.order_books[pair]
        async with book_state['lock']:
            if book_state['status'] == 'RESYNCING':
                return
            logger.info(f"Starting re-sync for {pair}...")
            book_state['status'] = 'RESYNCING'
            book_state['buffer'].clear()
        await self._initialize_book(pair)

    async def _initialize_book(self, pair: str) -> None:
        """Fetches a snapshot and synchronizes the order book."""
        book_state = self.order_books[pair]
        async with book_state['lock']:
            if book_state['status'] not in ['UNINITIALIZED', 'RESYNCING']:
                return
            book_state['status'] = 'SYNCING'

        snapshot = await self._fetch_snapshot_with_retry(pair)
        if not snapshot:
            async with book_state['lock']:
                book_state['status'] = 'UNINITIALIZED'
            return

        async with book_state['lock']:
            snapshot_id = snapshot['lastUpdateId']
            book_state['lastUpdateId'] = snapshot_id
            book_state['bids'] = {price: qty for price, qty in snapshot['bids']}
            book_state['asks'] = {price: qty for price, qty in snapshot['asks']}

            valid_buffer = [msg for msg in book_state['buffer'] if msg['u'] > snapshot_id]
            for diff in sorted(valid_buffer, key=lambda x: x['U']):
                await self._apply_diff_to_book(diff, book_state)

            book_state['buffer'].clear()
            book_state['status'] = 'SYNCED'
            logger.info(f"✅ Successfully synchronized order book for {pair}.")

    async def _fetch_snapshot_with_retry(self, pair: str) -> Optional[Dict[str, Any]]:
        """Fetches the initial order book snapshot with exponential backoff."""
        url = f"{self.config.binance_api_url}/depth?symbol={pair.upper()}&limit=30"
        for attempt in range(5):
            try:
                async with httpx.AsyncClient() as client:
                    res = await client.get(url, timeout=10)
                    res.raise_for_status()
                    return res.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Snapshot failed for {pair} (Invalid Symbol?). Status: {e.response.status_code}")
                self.order_books[pair]['status'] = 'FAILED'
                return None
            except httpx.RequestError as e:
                logger.error(f"Snapshot attempt {attempt+1} for {pair} failed: {e}.")
                if attempt == 4:
                    logger.critical(f"All snapshot attempts failed for {pair}.")
                    return None
                await asyncio.sleep(2 ** attempt)
        return None

    async def _save_snapshots_to_db(self) -> None:
        """Periodically saves snapshots to the database."""
        snapshot_time = datetime.now(timezone.utc)
        snapshot_batch: List[Tuple] = []

        for pair, book_state in self.order_books.items():
            async with book_state["lock"]:
                if book_state['status'] != 'SYNCED' or not book_state['bids'] or not book_state['asks']:
                    continue
                bids_copy = book_state['bids'].copy()
                asks_copy = book_state['asks'].copy()

            bids_list = sorted(bids_copy.items(), key=lambda x: Decimal(x[0]), reverse=True)
            asks_list = sorted(asks_copy.items(), key=lambda x: Decimal(x[0]))


            top_30_bids = bids_list[:30]
            top_30_asks = asks_list[:30]

            # --- THE FIX: Convert Decimals back to strings for JSON ---
            bids_to_save = [[str(price), str(qty)] for price, qty in top_30_bids]
            asks_to_save = [[str(price), str(qty)] for price, qty in top_30_asks]
            # ----------------------------------------------------------

            snapshot_batch.append(('binance', pair, json.dumps(bids_to_save), json.dumps(asks_to_save), snapshot_time))

        if not snapshot_batch or not self.db_pool:
            return

        try:
            async with self.db_pool.acquire() as conn:
                sql = "INSERT INTO orderbook_snapshots (exchange, symbol, bids, asks, time) VALUES ($1, $2, $3, $4, $5)"
                await conn.executemany(sql, snapshot_batch)
            logger.info(f"Successfully flushed {len(snapshot_batch)} snapshots to DB.")
        except (asyncpg.PostgresError, OSError) as e:
            logger.error(f"Database error on snapshot save: {e}", exc_info=True)

    async def _check_stream_health(self) -> None:
        """Watchdog to check for stale WebSocket connections."""
        now = time.time()
        for pair, book_state in self.order_books.items():
            if book_state['status'] == 'SYNCED':
                last_msg_time = self.last_message_times.get(pair, now)
                if now - last_msg_time > self.config.stale_connection_threshold_seconds:
                    logger.warning(f"Stale connection for {pair}. Re-syncing.")
                    self.last_message_times[pair] = now
                    asyncio.create_task(self._resync_book(pair))

    def shutdown(self):
        """Initiates a graceful shutdown."""
        logger.info("Shutdown signal received. Cleaning up...")
        self.shutdown_event.set()

    async def run(self) -> None:
        """The main entry point for running the collector."""
        self.loop = asyncio.get_running_loop()
        try:
            self.db_pool = await asyncpg.create_pool(
                user=self.config.db_user,
                password=self.config.db_password,
                database=self.config.db_name,
                host=self.config.db_host,
                min_size=1, max_size=5,
            )
            logger.info("Database connection pool created.")
        except (asyncpg.PostgresError, OSError) as e:
            logger.critical(f"FATAL: Could not connect to database: {e}")
            return

        logger.info(f"Starting collector for {len(self.config.target_pairs)} pairs: {self.config.target_pairs}")

        client = SpotWebsocketStreamClient(on_message=lambda _, msg: self._threadsafe_message_handler(msg), is_combined=True)
        client.subscribe(stream=[f"{pair}@depth" for pair in self.config.target_pairs])
        logger.info("Subscribed to WebSocket streams.")

        for pair in self.config.target_pairs:
            asyncio.create_task(self._initialize_book(pair))

        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(self._save_snapshots_to_db, 'interval', seconds=self.config.snapshot_interval_seconds)
        scheduler.add_job(self._check_stream_health, 'interval', seconds=self.config.watchdog_interval_seconds)
        scheduler.start()
        logger.info("Scheduler started.")

        try:
            await self.shutdown_event.wait()
        finally:
            scheduler.shutdown()
            client.stop()
            if self.db_pool:
                await self.db_pool.close()
            logger.info("Collector stopped gracefully.")


# --- 4. Clean Entry Point ---
# The main execution block is now minimal and easy to understand.
if __name__ == "__main__":
    try:
        config = load_config()
        collector = OrderBookCollector(config=config)
        asyncio.run(collector.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Application interrupted by user.")
    except Exception as e:
        logger.critical(f"A critical error occurred during startup or shutdown: {e}", exc_info=True)