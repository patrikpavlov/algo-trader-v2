import asyncio
import logging
import json
import os
import random
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, List, Optional, NamedTuple
import heapq  # Use heapq for efficient top-k selection

import asyncpg
import httpx
import redis.asyncio as redis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from pythonjsonlogger import jsonlogger
from redis.exceptions import RedisError

# --- Structured Logging Setup ---
log_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    '%(asctime)s %(name)s %(levelname)s %(message)s'
)
log_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(log_handler)
logger.setLevel(logging.INFO)
logger.propagate = False


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
    internal_queue_max_size: int
    # --- NEW: Backpressure and Concurrency Control ---
    stream_high_water_mark: int
    stream_low_water_mark: int
    max_concurrent_snapshots: int


def load_config() -> AppConfig:
    max_stream_len = 200000
    return AppConfig(
        redis_host=os.getenv("REDIS_HOST", "redis"),
        db_user=os.getenv("POSTGRES_USER"),
        db_password=os.getenv("POSTGRES_PASSWORD"),
        db_name=os.getenv("POSTGRES_DB"),
        db_host=os.getenv("DB_HOST", "db"),
        snapshot_interval_seconds=int(os.getenv("SNAPSHOT_INTERVAL_SECONDS", "1")),
        watchdog_interval_seconds=int(os.getenv("WATCHDOG_INTERVAL_SECONDS", "30")),
        stale_connection_threshold_seconds=int(os.getenv("STALE_CONNECTION_THRESHOLD_SECONDS", "60")),
        binance_api_url="https://api.binance.com/api/v3",
        max_stream_len=max_stream_len,
        internal_queue_max_size=int(os.getenv("INTERNAL_QUEUE_MAX_SIZE", "5000")),
        stream_high_water_mark=int(max_stream_len * 0.8),
        stream_low_water_mark=int(max_stream_len * 0.6),
        max_concurrent_snapshots=int(os.getenv("MAX_CONCURRENT_SNAPSHOTS", "10")),
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

        # --- IMPROVED: Asynchronous, decoupled components ---
        self.snapshot_queue: asyncio.Queue[List[Dict]] = asyncio.Queue(maxsize=self.config.internal_queue_max_size)
        self.collection_paused = asyncio.Event() # For backpressure
        self.snapshot_api_semaphore = asyncio.Semaphore(self.config.max_concurrent_snapshots) # Prevents thundering herd

    async def _fetch_active_symbols(self) -> List[str]:
        # (Unchanged - this logic is solid)
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
        # (Unchanged)
        self.order_books = {
            pair: {"bids": {}, "asks": {}, "lastUpdateId": None, "lock": asyncio.Lock(), "status": "UNINITIALIZED", "buffer": []}
            for pair in pairs
        }
        self.last_message_times = {pair: 0 for pair in pairs}

    def _threadsafe_message_handler(self, _, msg: str) -> None:
        # Using run_coroutine_threadsafe is correct for this library
        if self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._async_handle_message(msg), self.loop)

    async def _async_handle_message(self, msg_str: str) -> None:
        # (Unchanged - logic is sound)
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
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Failed to process websocket message", extra={'error': str(e), 'message': msg_str[:200]})
        except Exception as e:
            logger.error("Unexpected error in message handler", extra={'error': str(e)}, exc_info=True)

    async def _apply_diff_to_book(self, diff: Dict[str, Any], book_state: Dict[str, Any]):
        # (Unchanged - logic is sound)
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
            # The order of checks here is important for performance and correctness.
            if book_state['status'] != 'SYNCED': return
            if diff['u'] <= book_state['lastUpdateId']: return
            
            # If we have a gap, trigger a resync
            if diff['U'] > book_state['lastUpdateId'] + 1:
                logger.warning(
                    "Gap detected in order book stream. Triggering resync.",
                    extra={'pair': pair, 'last_id': book_state['lastUpdateId'], 'event_start_id': diff['U']}
                )
                asyncio.create_task(self._trigger_resync(pair))
                return
            
            await self._apply_diff_to_book(diff, book_state)

    async def _trigger_resync(self, pair: str) -> None:
        """
        NEW: Safely triggers a resync by acquiring a semaphore to prevent thundering herd.
        """
        book_state = self.order_books.get(pair)
        if not book_state: return

        # Avoid re-queueing if a resync is already pending or in progress
        async with book_state['lock']:
            if book_state['status'] in ['RESYNCING', 'SYNCING']:
                return
            logger.info("Queuing book for resynchronization.", extra={'pair': pair})
            book_state['status'] = 'RESYNCING'
            book_state['buffer'].clear()

        async with self.snapshot_api_semaphore:
            if self.shutdown_event.is_set(): return
            logger.info("Starting resync process.", extra={'pair': pair})
            await self._initialize_book(pair)

    async def _initialize_book(self, pair: str) -> None:
        """Handles the logic of fetching a snapshot and applying buffered diffs."""
        book_state = self.order_books.get(pair)
        if not book_state: return

        snapshot = await self._fetch_snapshot_with_retry(pair)
        if not snapshot:
            async with book_state['lock']:
                book_state['status'] = 'UNINITIALIZED' # Mark as failed
            return
        
        async with book_state['lock']:
            snapshot_id = snapshot['lastUpdateId']
            book_state['lastUpdateId'] = snapshot_id
            book_state['bids'] = {price: qty for price, qty in snapshot['bids']}
            book_state['asks'] = {price: qty for price, qty in snapshot['asks']}
            
            # Apply any diffs that arrived during the snapshot fetch
            valid_buffer = [msg for msg in book_state['buffer'] if msg['u'] > snapshot_id]
            for diff in sorted(valid_buffer, key=lambda x: x['U']):
                await self._apply_diff_to_book(diff, book_state)
            
            book_state['buffer'].clear()
            book_state['status'] = 'SYNCED'
            logger.info("Successfully synchronized order book.", extra={'pair': pair, 'final_update_id': book_state['lastUpdateId']})

    async def _fetch_snapshot_with_retry(self, pair: str) -> Optional[Dict[str, Any]]:
        # (Unchanged - this logic is solid)
        url = f"{self.config.binance_api_url}/depth?symbol={pair.upper()}&limit=1000"
        for attempt in range(5):
            try:
                async with httpx.AsyncClient() as client:
                    res = await client.get(url, timeout=10)
                    res.raise_for_status()
                    return res.json()
            except httpx.RequestError as e:
                logger.warning(f"Snapshot attempt failed.", extra={'pair': pair, 'attempt': attempt + 1, 'error': str(e)})
                if attempt < 4:
                    await asyncio.sleep(1 * (2 ** attempt)) # Exponential backoff
        logger.error(f"All snapshot attempts failed. Book will remain uninitialized.", extra={'pair': pair})
        return None

    async def _prepare_snapshot(self, pair: str, snapshot_time: float) -> Optional[Dict[str, Any]]:
        """
        IMPROVED: Uses heapq for high-performance top-k selection instead of full sort.
        This removes the need for a ThreadPoolExecutor.
        """
        book_state = self.order_books[pair]
        async with book_state["lock"]:
            if book_state['status'] != 'SYNCED' or not book_state['bids'] or not book_state['asks']:
                return None
            
            # Use Decimal for accurate price comparison in the heap
            bids_items = [(Decimal(p), q) for p, q in book_state['bids'].items()]
            asks_items = [(Decimal(p), q) for p, q in book_state['asks'].items()]

        # Efficiently get top 30 bids (highest price) and asks (lowest price)
        top_bids = heapq.nlargest(30, bids_items, key=lambda x: x[0])
        top_asks = heapq.nsmallest(30, asks_items, key=lambda x: x[0])

        return {
            "time": snapshot_time, "symbol": pair,
            "bids": json.dumps([[str(p), str(q)] for p, q in top_bids]),
            "asks": json.dumps([[str(p), str(q)] for p, q in top_asks])
        }

    async def _produce_snapshots_to_queue(self) -> None:
        """
        IMPROVED: Producer now respects backpressure.
        """
        if self.collection_paused.is_set():
            logger.info("Snapshot production is paused due to backpressure.")
            return

        if self.snapshot_queue.full():
            logger.warning("Internal snapshot queue is full, skipping this interval.", extra={'queue_size': self.snapshot_queue.qsize()})
            return

        snapshot_time = datetime.now(timezone.utc).timestamp()
        
        # Prepare snapshots concurrently
        tasks = [self._prepare_snapshot(pair, snapshot_time) for pair in self.order_books.keys()]
        if not tasks: return
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        snapshots_to_queue = [res for res in results if res is not None and not isinstance(res, Exception)]
        
        if snapshots_to_queue:
            try:
                self.snapshot_queue.put_nowait(snapshots_to_queue)
            except asyncio.QueueFull:
                # This can happen in a race condition, but is unlikely.
                logger.error("Internal queue became full unexpectedly during put. Snapshot batch dropped.")

    async def _redis_writer_worker(self) -> None:
        """
        IMPROVED: Non-blocking writer that re-queues failed batches.
        This is the most critical improvement for preventing data loss.
        """
        logger.info("Starting Redis writer worker.")
        while not self.shutdown_event.is_set():
            try:
                snapshots_to_send = await self.snapshot_queue.get()
                
                try:
                    pipe = self.redis_client.pipeline()
                    for snapshot in snapshots_to_send:
                        pipe.xadd("snapshots_stream", {"data": json.dumps(snapshot)}, maxlen=self.config.max_stream_len)
                    await asyncio.wait_for(pipe.execute(), timeout=10.0)
                    self.snapshot_queue.task_done()
                except (RedisError, asyncio.TimeoutError) as e:
                    logger.error("Redis write failed. Re-queuing snapshot batch.", extra={'error': str(e), 'batch_size': len(snapshots_to_send)})
                    # Re-queue the failed batch to be tried again later.
                    await self.snapshot_queue.put(snapshots_to_send) 
                    await asyncio.sleep(5) # Wait before processing next item to avoid tight loop on persistent failure
                except Exception as e:
                    logger.critical("Unhandled writer error. Re-queuing batch.", extra={'error': str(e)}, exc_info=True)
                    await self.snapshot_queue.put(snapshots_to_send)
                    await asyncio.sleep(5)

            except asyncio.CancelledError:
                break
        logger.info("Redis writer worker has stopped.")

    async def _fetch_binance_symbols(self) -> set:
        """Fetches all active trading symbols directly from the Binance API."""
        url = f"{self.config.binance_api_url}/exchangeInfo"
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(url, timeout=10)
                res.raise_for_status()
                data = res.json()
                # Return a set of lowercase symbols for efficient lookup
                return {s['symbol'].lower() for s in data['symbols'] if s['status'] == 'TRADING'}
        except httpx.RequestError as e:
            logger.critical(f"Could not fetch exchange info from Binance: {e}")
            return set()

    async def _monitor_stream_backlog(self):
        """
        Monitors the number of unacknowledged messages in a consumer group
        to apply/release backpressure. This is the true measure of consumer lag.
        """
        if not self.redis_client: return
        try:
            # Check the number of pending messages for our consumer group
            # This represents the true backlog of unprocessed snapshots.
            pending_info = await self.redis_client.xpending("snapshots_stream", "snapshots_group")
            pending_count = pending_info['pending']

            if pending_count > self.config.stream_high_water_mark and not self.collection_paused.is_set():
                self.collection_paused.set()
                logger.warning(
                    f"BACKPRESSURE APPLIED: Pending messages ({pending_count}) exceed high water mark.",
                    extra={'pending_count': pending_count, 'high_water_mark': self.config.stream_high_water_mark}
                )

            elif pending_count < self.config.stream_low_water_mark and self.collection_paused.is_set():
                self.collection_paused.clear()
                logger.info(
                    f"BACKPRESSURE RELEASED: Pending messages ({pending_count}) are below low water mark.",
                    extra={'pending_count': pending_count, 'low_water_mark': self.config.stream_low_water_mark}
                )

        except RedisError as e:
            # This can happen if the stream or group doesn't exist yet.
            if "No such key" in str(e) or "No such group" in str(e):
                logger.info("Stream or consumer group not yet created by the consumer. Skipping backlog check.")
            else:
                logger.error(f"Could not monitor Redis stream backlog: {e}")

    async def _log_health_metrics(self) -> None:
        """NEW: Comprehensive health logging."""
        status = "PAUSED (Backpressure)" if self.collection_paused.is_set() else "ACTIVE"
        
        status_counts = {"SYNCED": 0, "SYNCING": 0, "RESYNCING": 0, "UNINITIALIZED": 0}
        for book in self.order_books.values():
            status_counts[book['status']] += 1
        
        redis_stream_len = -1
        if self.redis_client:
            try:
                pending_info = await self.redis_client.xpending("snapshots_stream", "snapshots_group")
                pending_count = pending_info['pending']
                redis_stream_len = pending_count
            except RedisError:
                pass # Already logged in monitor task

        logger.info(
            "Health Check",
            extra={
                'status': status,
                'snapshot_queue_size': self.snapshot_queue.qsize(),
                'redis_stream_length': redis_stream_len,
                'book_statuses': status_counts,
                'api_calls_in_progress': self.snapshot_api_semaphore._value < self.config.max_concurrent_snapshots
            }
        )
        
        # Also touch a file for simple container health checks
        try:
            with open("/tmp/heartbeat_book", "w") as f:
                f.write(str(int(time.time())))
        except IOError as e:
            logger.warning("Could not write heartbeat file", extra={'error': str(e)})


    async def _check_stream_health(self) -> None:
        now = time.time()
        for pair, last_msg_time in self.last_message_times.items():
            if now - last_msg_time > self.config.stale_connection_threshold_seconds:
                book_state = self.order_books.get(pair, {})
                # Only resync if we think we are synced. Otherwise, let initial sync finish.
                if book_state.get('status') == 'SYNCED':
                    logger.warning("Stale websocket data detected for SYNCED book. Triggering resync.", extra={'pair': pair})
                    self.last_message_times[pair] = now # Reset timer to prevent rapid re-triggers
                    asyncio.create_task(self._trigger_resync(pair))

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        try:
            # --- DB Connection (Corrected from before) ---
            db_conn_info = {
                "user": self.config.db_user, "password": self.config.db_password,
                "database": self.config.db_name, "host": self.config.db_host
            }
            self.db_pool = await asyncpg.create_pool(**db_conn_info)
            
            # 1. Fetch symbols from your database
            symbols_from_db = await self._fetch_active_symbols()
            await self.db_pool.close()

            # 2. Fetch all valid symbols from Binance
            valid_binance_symbols = await self._fetch_binance_symbols()
            if not valid_binance_symbols:
                logger.critical("Could not get valid symbols from Binance. Shutting down.")
                return

            # 3. Compare lists and filter out invalid symbols
            target_pairs = [s for s in symbols_from_db if s in valid_binance_symbols]
            invalid_pairs = set(symbols_from_db) - set(target_pairs)

            # 4. Log any invalid symbols found
            if invalid_pairs:
                logger.warning(
                    "Invalid symbols found in config and will be ignored.",
                    extra={'invalid_symbols': list(invalid_pairs)}
                )
                
            if not target_pairs:
                logger.critical("No valid symbols to monitor after filtering. Shutting down.")
                return

            # --- Proceed with the rest of the startup using the validated 'target_pairs' list ---
            self._initialize_book_states(target_pairs)
            self.redis_client = redis.from_url(f"redis://{self.config.redis_host}", health_check_interval=30)
            await self.redis_client.ping()

        except Exception as e:
            logger.critical("A critical startup connection failed. Exiting.", extra={'error': str(e)}, exc_info=True)
            return

        logger.info(f"Starting book collector for {len(target_pairs)} pairs.")
        client = SpotWebsocketStreamClient(on_message=self._threadsafe_message_handler, is_combined=True)
        client.subscribe(stream=[f"{pair}@depth" for pair in target_pairs])
        
        # Stagger initial snapshot fetches to avoid thundering herd on startup
        for pair in target_pairs:
            asyncio.create_task(self._staggered_initial_sync(pair))

        writer_task = asyncio.create_task(self._redis_writer_worker())
        
        scheduler = AsyncIOScheduler(timezone="UTC", job_defaults={'misfire_grace_time': 5, 'coalesce': True})
        scheduler.add_job(self._produce_snapshots_to_queue, 'interval', seconds=self.config.snapshot_interval_seconds)
        scheduler.add_job(self._check_stream_health, 'interval', seconds=self.config.watchdog_interval_seconds)
        scheduler.add_job(self._monitor_stream_backlog, 'interval', seconds=10)
        scheduler.add_job(self._log_health_metrics, 'interval', seconds=15)
        scheduler.start()

        try:
            await self.shutdown_event.wait()
        finally:
            logger.info("Shutdown signal received. Gracefully stopping...")
            self.shutdown_event.set()
            if scheduler.running: scheduler.shutdown()
            if client: client.stop()
            
            # Wait for the writer to finish processing the queue
            try:
                logger.info(f"Draining internal queue of {self.snapshot_queue.qsize()} items...")
                await asyncio.wait_for(self.snapshot_queue.join(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.error("Timeout while draining queue. Some snapshot data may be lost.")
            
            # Cancel the writer task and clean up connections
            writer_task.cancel()
            await asyncio.gather(writer_task, return_exceptions=True)
            if self.redis_client: await self.redis_client.close()
            logger.info("Collector stopped.")

    async def _staggered_initial_sync(self, pair: str):
        # Stagger startup to prevent hitting API rate limits
        await asyncio.sleep(random.uniform(0, min(len(self.order_books), 10)))
        await self._trigger_resync(pair)

if __name__ == "__main__":
    try:
        config = load_config()
        collector = OrderBookCollector(config=config)
        asyncio.run(collector.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Application interrupted by user.")