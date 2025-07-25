import asyncio
import logging
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, List, Optional, NamedTuple

import asyncpg
import httpx
import redis.asyncio as redis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("book_collector.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


# --- Configuration ---
class AppConfig(NamedTuple):
    redis_host: str
    db_user: Optional[str]
    db_password: Optional[str]
    db_name: Optional[str]
    db_host: Optional[str]
    snapshot_interval_seconds: int
    watchdog_interval_seconds: int
    stale_connection_threshold_seconds: int
    binance_api_url: str
    max_stream_len: int

def load_config() -> AppConfig:
    return AppConfig(
        redis_host=os.getenv("REDIS_HOST", "redis"),
        db_user=os.getenv("POSTGRES_USER"),
        db_password=os.getenv("POSTGRES_PASSWORD"),
        db_name=os.getenv("POSTGRES_DB"),
        db_host=os.getenv("DB_HOST", "db"),
        snapshot_interval_seconds=int(os.getenv("SNAPSHOT_INTERVAL", "1")),
        watchdog_interval_seconds=int(os.getenv("WATCHDOG_INTERVAL", "15")),
        stale_connection_threshold_seconds=int(os.getenv("STALE_CONNECTION_THRESHOLD", "60")),
        binance_api_url="https://api.binance.com/api/v3",
        max_stream_len=int(os.getenv("MAX_STREAM_LEN", "100000")),
    )


# --- Application Logic ---
class OrderBookCollector:
    def __init__(self, config: AppConfig):
        self.config = config
        self.redis_client: Optional[redis.Redis] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.shutdown_event = asyncio.Event()
        self.order_books: Dict[str, Dict[str, Any]] = {}
        self.last_message_times: Dict[str, float] = {}

    async def _fetch_active_symbols(self) -> List[str]:
        if not self.db_pool:
            logger.error("Database pool is not available.")
            return []
        try:
            async with self.db_pool.acquire() as conn:
                records = await conn.fetch("SELECT symbol FROM monitored_symbols WHERE is_active = true")
                symbols = [rec['symbol'] for rec in records]
                logger.info(f"Loaded {len(symbols)} active symbols from database.")
                return symbols
        except Exception as e:
            logger.critical(f"Could not fetch symbols from database: {e}")
            return []

    def _initialize_book_states(self, pairs: List[str]):
        self.order_books = {
            pair: {"bids": {}, "asks": {}, "lastUpdateId": None, "lock": asyncio.Lock(), "status": "UNINITIALIZED", "buffer": []}
            for pair in pairs
        }
        self.last_message_times = {pair: 0 for pair in pairs}

    def _threadsafe_message_handler(self, msg: str) -> None:
        if self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._async_handle_message(msg), self.loop)

    async def _async_handle_message(self, msg_str: str) -> None:
        try:
            msg = json.loads(msg_str)
            diff_payload = msg.get('data', {})
            if diff_payload.get('e') != 'depthUpdate': return
            pair = diff_payload['s'].lower()
            if pair not in self.order_books: return
            self.last_message_times[pair] = time.time()
            book_state = self.order_books[pair]
            if book_state['status'] in ['UNINITIALIZED', 'SYNCING', 'RESYNCING']:
                async with book_state['lock']: book_state['buffer'].append(diff_payload)
            elif book_state['status'] == 'SYNCED':
                await self._apply_diff(diff_payload)
        except Exception as e:
            logger.error(f"Error in message handler: {e}", exc_info=True)

    async def _apply_diff_to_book(self, diff: Dict[str, Any], book_state: Dict[str, Any]):
        for price, qty in diff['b']:
            if Decimal(qty) == Decimal('0'): book_state['bids'].pop(price, None)
            else: book_state['bids'][price] = qty
        for price, qty in diff['a']:
            if Decimal(qty) == Decimal('0'): book_state['asks'].pop(price, None)
            else: book_state['asks'][price] = qty
        book_state['lastUpdateId'] = diff['u']

    async def _apply_diff(self, diff: Dict[str, Any]) -> None:
        pair = diff['s'].lower()
        book_state = self.order_books[pair]
        async with book_state['lock']:
            if book_state['status'] != 'SYNCED' or diff['u'] <= book_state['lastUpdateId']: return
            if diff['U'] > book_state['lastUpdateId'] + 1:
                logger.warning(f"Gap detected for {pair}. Triggering re-sync.")
                asyncio.create_task(self._resync_book(pair))
                return
            await self._apply_diff_to_book(diff, book_state)

    async def _resync_book(self, pair: str) -> None:
        book_state = self.order_books.get(pair)
        if not book_state: return
        async with book_state['lock']:
            if book_state['status'] == 'RESYNCING': return
            logger.info(f"Starting re-sync for {pair}...")
            book_state['status'] = 'RESYNCING'
            book_state['buffer'].clear()
        await self._initialize_book(pair)

    async def _initialize_book(self, pair: str) -> None:
        book_state = self.order_books.get(pair)
        if not book_state: return
        async with book_state['lock']:
            if book_state['status'] not in ['UNINITIALIZED', 'RESYNCING']: return
            book_state['status'] = 'SYNCING'
        snapshot = await self._fetch_snapshot_with_retry(pair)
        if not snapshot:
            async with book_state['lock']: book_state['status'] = 'UNINITIALIZED'
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
            logger.info(f"Successfully synchronized order book for {pair}.")

    async def _fetch_snapshot_with_retry(self, pair: str) -> Optional[Dict[str, Any]]:
        url = f"{self.config.binance_api_url}/depth?symbol={pair.upper()}&limit=30"
        for attempt in range(5):
            try:
                async with httpx.AsyncClient() as client:
                    res = await client.get(url, timeout=10)
                    res.raise_for_status()
                    return res.json()
            except httpx.RequestError as e:
                logger.warning(f"Snapshot attempt {attempt+1} for {pair} failed: {e}.")
                if attempt == 4:
                    logger.critical(f"All snapshot attempts failed for {pair}.")
                    return None
                await asyncio.sleep(2 ** attempt)
        return None

    async def _check_stream_health(self) -> None:
        now = time.time()
        for pair, book_state in self.order_books.items():
            if book_state.get('status') == 'SYNCED':
                last_msg_time = self.last_message_times.get(pair, now)
                if now - last_msg_time > self.config.stale_connection_threshold_seconds:
                    logger.warning(f"Stale connection for {pair}. Re-syncing.")
                    self.last_message_times[pair] = now
                    asyncio.create_task(self._resync_book(pair))

    async def _save_snapshots_to_redis(self) -> None:
        if not self.redis_client: return
        pipe = self.redis_client.pipeline()
        count = 0
        snapshot_time = datetime.now(timezone.utc).timestamp()
        snapshots_to_send = []
        for pair, book_state in self.order_books.items():
            async with book_state["lock"]:
                if book_state['status'] != 'SYNCED' or not book_state['bids'] or not book_state['asks']: continue
                bids_copy = book_state['bids'].copy()
                asks_copy = book_state['asks'].copy()
            bids_list = sorted(bids_copy.items(), key=lambda x: Decimal(x[0]), reverse=True)[:30]
            asks_list = sorted(asks_copy.items(), key=lambda x: Decimal(x[0]))[:30]
            snapshot_data = {
                "time": snapshot_time, "symbol": pair,
                "bids": json.dumps([[str(p), str(q)] for p, q in bids_list]),
                "asks": json.dumps([[str(p), str(q)] for p, q in asks_list])
            }
            snapshots_to_send.append(snapshot_data)
            count += 1
        if count == 0: return
        for snapshot in snapshots_to_send:
            pipe.xadd("snapshots_stream", {"data": json.dumps(snapshot)}, maxlen=self.config.max_stream_len, approximate=True)
        for attempt in range(5):
            try:
                await pipe.execute()
                logger.info(f"Successfully flushed {count} snapshots to Redis.")
                return
            except redis.RedisError as e:
                logger.warning(f"Snapshot flush attempt {attempt+1} failed: {e}")
                if attempt == 4:
                    logger.critical("All snapshot flush attempts failed.")
                    break
                await asyncio.sleep(1 * (2 ** attempt))

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        try:
            self.db_pool = await asyncpg.create_pool(
                user=self.config.db_user, password=self.config.db_password,
                database=self.config.db_name, host=self.config.db_host
            )
            target_pairs = await self._fetch_active_symbols()
            await self.db_pool.close()
            if not target_pairs:
                logger.critical("No active symbols to monitor. Shutting down.")
                return
            self._initialize_book_states(target_pairs)
            self.redis_client = redis.from_url(f"redis://{self.config.redis_host}")
            await self.redis_client.ping()
            logger.info("Connected to Redis successfully.")
        except Exception as e:
            logger.critical(f"FATAL: A startup connection failed: {e}")
            return

        logger.info(f"Starting book collector for {len(target_pairs)} pairs.")
        client = SpotWebsocketStreamClient(on_message=lambda _, msg: self._threadsafe_message_handler(msg), is_combined=True)
        client.subscribe(stream=[f"{pair}@depth" for pair in target_pairs])
        for pair in target_pairs:
            asyncio.create_task(self._initialize_book(pair))
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(self._save_snapshots_to_redis, 'interval', seconds=self.config.snapshot_interval_seconds)
        scheduler.add_job(self._check_stream_health, 'interval', seconds=self.config.watchdog_interval_seconds)
        scheduler.start()
        logger.info("Collector is running.")
        try:
            await self.shutdown_event.wait()
        finally:
            logger.info("Shutting down...")
            scheduler.shutdown()
            client.stop()
            if self.redis_client: await self.redis_client.close()
            logger.info("Collector stopped.")

if __name__ == "__main__":
    try:
        config = load_config()
        collector = OrderBookCollector(config=config)
        asyncio.run(collector.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Application interrupted by user.")