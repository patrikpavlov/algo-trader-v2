import asyncio
import logging
import json
import os
import sys
import time
from typing import List, Optional, NamedTuple
from collections import deque # Use deque for efficient appends/pops

import asyncpg
import redis.asyncio as redis
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- Configuration (Unchanged from your original) ---
class AppConfig(NamedTuple):
    redis_host: str
    db_user: Optional[str]
    db_password: Optional[str]
    db_name: Optional[str]
    db_host: Optional[str]
    batch_size: int
    flush_interval_seconds: int
    stale_connection_threshold_seconds: int
    max_stream_len: int
    stream_high_water_mark: int
    stream_low_water_mark: int

def load_config() -> AppConfig:
    max_stream_len = int(os.getenv("MAX_STREAM_LEN", "1000000"))
    return AppConfig(
        redis_host=os.getenv("REDIS_HOST", "redis"),
        db_user=os.getenv("POSTGRES_USER"),
        db_password=os.getenv("POSTGRES_PASSWORD"),
        db_name=os.getenv("POSTGRES_DB"),
        db_host=os.getenv("DB_HOST", "db"),
        batch_size=int(os.getenv("BATCH_SIZE", "5000")), # Changed back to 5000 based on previous discussion
        flush_interval_seconds=int(os.getenv("FLUSH_INTERVAL", "2")), # Changed back to 2 based on previous discussion
        stale_connection_threshold_seconds=int(os.getenv("STALE_CONNECTION_THRESHOLD", "300")), # 5 minutes
        max_stream_len=int(os.getenv("MAX_STREAM_LEN", "2000000")), # Changed back to 2000000 based on previous discussion
        stream_high_water_mark=int(max_stream_len * 0.8),
        stream_low_water_mark=int(max_stream_len * 0.6)
    )

# --- Application Logic ---
class TradeCollector:
    def __init__(self, config: AppConfig):
        self.config = config
        self.redis_client: Optional[redis.Redis] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.ws_client: Optional[SpotWebsocketStreamClient] = None
        self.target_pairs: List[str] = []
        
        self.thread_safe_buffer = deque() # Collects raw messages from websocket thread
        self.trade_buffer = deque()       # Main buffer for processed trades
        self.buffer_lock = asyncio.Lock() # Lock for trade_buffer
        
        self.flush_lock = asyncio.Lock()  # Your original flush lock
        self.last_message_buffered_time = 0.0 # Renamed for clarity
        self.reconnecting = asyncio.Event() 
        self.collection_paused = asyncio.Event()

    async def _touch_heartbeat(self): # Changed to async def
        """
        Creates or updates a heartbeat file to signal liveness and
        logs the internal state of the collector, including Redis stream length.
        """
        # 1. Write the heartbeat file for Docker
        try:
            with open("/tmp/heartbeat", "w") as f:
                f.write("healthy")
        except Exception as e:
            logger.warning(f"Could not write heartbeat file: {e}")

        # --- NEW: Log internal queue sizes for monitoring ---
        thread_buffer_size = len(self.thread_safe_buffer)
        main_buffer_size = len(self.trade_buffer)
        
        # Get Redis stream length
        redis_stream_len = -1
        # Removed .is_connected check to fix AttributeError
        if self.redis_client:
            try:
                redis_stream_len = await self.redis_client.xpending("trades_stream", "trades_group")
                if isinstance(redis_stream_len, dict) and 'pending' in redis_stream_len and isinstance(redis_stream_len['pending'], int):
                    redis_stream_len = redis_stream_len['pending']

            except (RedisConnectionError, RedisTimeoutError, Exception) as e: # Catch specific Redis errors and general exceptions
                logger.warning(f"Could not get Redis stream length (client might be disconnected): {e}")
                # Optionally, you could trigger a Redis reconnect here if needed, but _connect_to_redis is generally called at startup
                # and _monitor_stream_backlog handles errors by re-establishing connections.
        
        # Determine status based on whether backpressure is active
        status = "PAUSED (Backpressure)" if self.collection_paused.is_set() else "ACTIVE"

        # Log a single, comprehensive line with all key metrics.
        logger.info(
            f"[HEALTH_CHECK] Status: {status} | "
            f"Websocket Buffer: {thread_buffer_size} | "
            f"Flush Buffer: {main_buffer_size} | "
            f"Redis Stream Length: {redis_stream_len}" # Added Redis Stream Length
        )

    async def _monitor_stream_backlog(self):
        if not self.redis_client: return
        try:

            stream_len = await self.redis_client.xpending("trades_stream", "trades_group")
            if isinstance(stream_len, dict) and 'pending' in stream_len and isinstance(stream_len['pending'], int):
                stream_len = stream_len['pending']
            
            # Check if we need to PAUSE collection
            if stream_len > self.config.stream_high_water_mark and not self.collection_paused.is_set():
                self.collection_paused.set() # Set the event to pause collection
                logger.warning(f"BACKPRESSURE APPLIED: Redis stream backlog ({stream_len}) exceeds high water mark ({self.config.stream_high_water_mark}). Pausing data collection.")
            
            # Check if we can RESUME collection
            elif stream_len < self.config.stream_low_water_mark and self.collection_paused.is_set():
                self.collection_paused.clear() # Clear the event to resume collection
                logger.info(f"BACKPRESSURE RELEASED: Redis stream backlog ({stream_len}) is below low water mark ({self.config.stream_low_water_mark}). Resuming data collection.")

        except (RedisConnectionError, RedisTimeoutError, Exception) as e: # Catch specific Redis errors
            logger.error(f"Could not monitor Redis stream backlog: {e}")
            
    async def _connect_to_redis(self):
        # Adding resiliency options from our previous findings
        self.redis_client = redis.from_url(
            f"redis://{self.config.redis_host}",
            socket_connect_timeout=10, socket_keepalive=True, health_check_interval=30
        )
        await self.redis_client.ping()
        logger.info("Connected to Redis successfully.")

    async def _fetch_active_symbols(self) -> List[str]:
        conn = await asyncpg.connect(
            user=self.config.db_user, password=self.config.db_password,
            database=self.config.db_name, host=self.config.db_host
        )
        try:
            records = await conn.fetch("SELECT symbol FROM monitored_symbols WHERE is_active = true")
            self.target_pairs = [rec['symbol'] for rec in records]
            return self.target_pairs
        finally:
            await conn.close()

    def _threadsafe_message_handler(self, _, msg: str):
        # Original: self.last_message_time = time.time() - This should be last_message_buffered_time
        # and handled in _process_incoming_messages
        self.thread_safe_buffer.append(msg)

    async def _process_incoming_messages(self):
        """New task to efficiently move messages from thread buffer to main buffer."""
        while True:
            if not self.thread_safe_buffer:
                await asyncio.sleep(0.01)
                continue

            # Drain the entire thread-safe buffer at once
            messages_to_process = []
            while self.thread_safe_buffer:
                messages_to_process.append(self.thread_safe_buffer.popleft())

            # If collection is paused, drop the batch and continue
            if self.collection_paused.is_set():
                continue

            # Correctly update health timer - after messages are accepted for processing
            self.last_message_buffered_time = time.time()
            
            # Process the batch of messages
            processed_trades = []
            for msg_str in messages_to_process:
                try:
                    msg = json.loads(msg_str)
                    payload = msg.get('data')
                    if not payload or payload.get('e') != 'trade': continue
                    processed_trades.append({
                        "time": payload['T'] / 1000.0, "symbol": payload['s'].lower(), "trade_id": payload['t'],
                        "price": str(payload['p']), "amount": str(payload['q']), "is_buyer_maker": payload['m']
                    })
                except (json.JSONDecodeError, KeyError): continue
            
            # Add the processed trades to the main buffer under one lock
            if processed_trades:
                async with self.buffer_lock:
                    self.trade_buffer.extend(processed_trades)
                    should_flush = len(self.trade_buffer) >= self.config.batch_size
                if should_flush:
                    asyncio.create_task(self._flush_trades_to_redis())

    async def _flush_trades_to_redis(self):
        async with self.flush_lock:
            async with self.buffer_lock:
                if not self.trade_buffer: return
                trades_to_flush = [self.trade_buffer.popleft() for _ in range(len(self.trade_buffer))] # Use deque.popleft()
            try:
                pipe = self.redis_client.pipeline()
                for trade in trades_to_flush:
                    pipe.xadd("trades_stream", {"data": json.dumps(trade)}, maxlen=self.config.max_stream_len, approximate=True)
                await asyncio.wait_for(pipe.execute(), timeout=5.0) # Add timeout
                logger.info(f"Successfully flushed {len(trades_to_flush)} trades to Redis.")
            except asyncio.TimeoutError:
                logger.error(f"Redis flush TIMED OUT. Re-buffering {len(trades_to_flush)} trades.")
                async with self.buffer_lock:
                    self.trade_buffer.extendleft(reversed(trades_to_flush)) # Re-buffer on timeout
            except Exception as e:
                logger.error(f"Failed to flush trades to Redis: {e}. Re-buffering.")
                async with self.buffer_lock:
                    self.trade_buffer.extendleft(reversed(trades_to_flush))

    async def _check_stream_health(self):
        if self.reconnecting.is_set(): return # Don't check health during reconnection
        
        time_since_last_buffer = time.time() - self.last_message_buffered_time
        if time_since_last_buffer > self.config.stale_connection_threshold_seconds:
            logger.warning(f"Stale connection detected ({time_since_last_buffer:.0f}s). Initiating automatic reconnect.")
            self.reconnecting.set() # Signal that we are starting a reconnect
            asyncio.create_task(self._reconnect_client())
        else:
            logger.debug(f"HEALTH CHECK: Trades stream is active. Last message buffered {time_since_last_buffer:.2f}s ago.") # Changed to DEBUG

    async def _reconnect_client(self):
        if self.ws_client:
            self.ws_client.stop()
            logger.info("Old WebSocket client stopped.")
        
        await asyncio.sleep(5) # Brief pause
        
        logger.info("Attempting to start a new WebSocket client...")
        self.ws_client = SpotWebsocketStreamClient(on_message=self._threadsafe_message_handler, is_combined=True)
        self.ws_client.subscribe(stream=[f"{pair}@trade" for pair in self.target_pairs])
        self.last_message_buffered_time = time.time() # Reset timer
        self.reconnecting.clear() # Signal that reconnection is complete
        logger.info("Reconnection complete. New client is running.")

    async def run(self):
        self.loop = asyncio.get_running_loop()
        try:
            await self._connect_to_redis()
            await self._fetch_active_symbols()
        except Exception as e:
            logger.critical(f"FATAL: Could not connect to services on startup: {e}", exc_info=True)
            sys.exit(1) # Exit if critical startup connection fails
        
        self.last_message_buffered_time = time.time()
        self.ws_client = SpotWebsocketStreamClient(on_message=self._threadsafe_message_handler, is_combined=True)
        self.ws_client.subscribe(stream=[f"{pair}@trade" for pair in self.target_pairs])
        
        # Start the new message processing task
        processing_task = asyncio.create_task(self._process_incoming_messages())

        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(self._flush_trades_to_redis, 'interval', seconds=self.config.flush_interval_seconds)
        scheduler.add_job(self._check_stream_health, 'interval', seconds=60)
        scheduler.add_job(self._monitor_stream_backlog, 'interval', seconds=10)
        scheduler.add_job(self._touch_heartbeat, 'interval', seconds=15) # This job is now async

        scheduler.start()

        logger.info(f"Trade collector is running for {len(self.target_pairs)} pairs.")
        
        # Keep the main coroutine alive by waiting on the processing task
        try:
            await processing_task
        except asyncio.CancelledError:
            pass # Task was cancelled, graceful shutdown
        finally:
            # Ensure the processing task is cancelled if still running
            if not processing_task.done():
                processing_task.cancel()
            if scheduler.running:
                scheduler.shutdown()
            if self.ws_client:
                self.ws_client.stop()
            logger.info("Collector shutdown complete.")


if __name__ == "__main__":
    try:
        collector = TradeCollector(load_config())
        asyncio.run(collector.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Collector service stopped by user.")
    except Exception as e:
        logger.critical(f"Collector service exited due to an unhandled exception: {e}", exc_info=True)