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
        batch_size=int(os.getenv("BATCH_SIZE", "200")),
        flush_interval_seconds=int(os.getenv("FLUSH_INTERVAL", "5")),
        stale_connection_threshold_seconds=int(os.getenv("STALE_CONNECTION_THRESHOLD", "300")), # 5 minutes
        max_stream_len=int(os.getenv("MAX_STREAM_LEN", "1000000")),
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
        
        self.trade_buffer: list = []
        self.buffer_lock = asyncio.Lock()
        self.flush_lock = asyncio.Lock()
        self.last_message_time = 0.0
        self.reconnecting = asyncio.Event() 
        self.collection_paused = asyncio.Event()

    async def _monitor_stream_backlog(self):
        if not self.redis_client: return
        try:
            stream_len = await self.redis_client.xlen("trades_stream")
            
            # Check if we need to PAUSE collection
            if stream_len > self.config.stream_high_water_mark and not self.collection_paused.is_set():
                self.collection_paused.set() # Set the event to pause collection
                logger.warning(f"BACKPRESSURE APPLIED: Redis stream backlog ({stream_len}) exceeds high water mark ({self.config.stream_high_water_mark}). Pausing data collection.")
            
            # Check if we can RESUME collection
            elif stream_len < self.config.stream_low_water_mark and self.collection_paused.is_set():
                self.collection_paused.clear() # Clear the event to resume collection
                logger.info(f"BACKPRESSURE RELEASED: Redis stream backlog ({stream_len}) is below low water mark ({self.config.stream_low_water_mark}). Resuming data collection.")

        except Exception as e:
            logger.error(f"Could not monitor Redis stream backlog: {e}")
            
    async def _connect_to_redis(self):
        self.redis_client = redis.from_url(f"redis://{self.config.redis_host}")
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
        self.last_message_time = time.time()
        if self.loop and not self.loop.is_closed():
            if not self.collection_paused.is_set():
                asyncio.run_coroutine_threadsafe(self._process_message(msg), self.loop)

    async def _process_message(self, msg_str: str):
        try:
            msg = json.loads(msg_str)
            trade_payload = msg.get('data')
            if not trade_payload or trade_payload.get('e') != 'trade':
                return
            trade_data = { "time": trade_payload['T'] / 1000.0, "symbol": trade_payload['s'].lower(), "trade_id": trade_payload['t'], "price": str(trade_payload['p']), "amount": str(trade_payload['q']), "is_buyer_maker": trade_payload['m'] }
            async with self.buffer_lock:
                self.trade_buffer.append(trade_data)
                should_flush = len(self.trade_buffer) >= self.config.batch_size
            if should_flush:
                asyncio.create_task(self._flush_trades_to_redis())
        except Exception as e:
            logger.error(f"Error in message processor: {e}", exc_info=True)

    async def _flush_trades_to_redis(self):
        async with self.flush_lock:
            async with self.buffer_lock:
                if not self.trade_buffer: return
                trades_to_flush = list(self.trade_buffer)
                self.trade_buffer.clear()
            try:
                pipe = self.redis_client.pipeline()
                for trade in trades_to_flush:
                    pipe.xadd("trades_stream", {"data": json.dumps(trade)}, maxlen=self.config.max_stream_len, approximate=True)
                await pipe.execute()
                logger.info(f"Successfully flushed {len(trades_to_flush)} trades to Redis.")
            except Exception as e:
                logger.error(f"Failed to flush trades to Redis: {e}. Re-buffering.")
                async with self.buffer_lock:
                    self.trade_buffer = trades_to_flush + self.trade_buffer

    async def _check_stream_health(self):
        if self.reconnecting.is_set(): return # Don't check health during reconnection
        
        time_since_last_msg = time.time() - self.last_message_time
        if time_since_last_msg > self.config.stale_connection_threshold_seconds:
            logger.warning(f"Stale connection detected ({time_since_last_msg:.0f}s). Initiating automatic reconnect.")
            self.reconnecting.set() # Signal that we are starting a reconnect
            asyncio.create_task(self._reconnect_client())
        else:
            logger.info(f"HEALTH CHECK: Trades stream is active. Last message {time_since_last_msg:.2f}s ago.")
    
    async def _reconnect_client(self):
        if self.ws_client:
            self.ws_client.stop()
            logger.info("Old WebSocket client stopped.")
        
        await asyncio.sleep(5) # Brief pause
        
        logger.info("Attempting to start a new WebSocket client...")
        self.ws_client = SpotWebsocketStreamClient(on_message=self._threadsafe_message_handler, is_combined=True)
        self.ws_client.subscribe(stream=[f"{pair}@trade" for pair in self.target_pairs])
        self.last_message_time = time.time() # Reset timer
        self.reconnecting.clear() # Signal that reconnection is complete
        logger.info("Reconnection complete. New client is running.")

    async def run(self):
        self.loop = asyncio.get_running_loop()
        try:
            await self._connect_to_redis()
            await self._fetch_active_symbols()
        except Exception as e:
            logger.critical(f"FATAL: Could not connect to services on startup: {e}", exc_info=True)
            return
        
        self.last_message_time = time.time()
        self.ws_client = SpotWebsocketStreamClient(on_message=self._threadsafe_message_handler, is_combined=True)
        self.ws_client.subscribe(stream=[f"{pair}@trade" for pair in self.target_pairs])
        
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(self._flush_trades_to_redis, 'interval', seconds=self.config.flush_interval_seconds)
        scheduler.add_job(self._check_stream_health, 'interval', seconds=60)
        scheduler.add_job(self._monitor_stream_backlog, 'interval', seconds=10)
        scheduler.start()

        logger.info(f"Trade collector is running for {len(self.target_pairs)} pairs.")
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        collector = TradeCollector(load_config())
        asyncio.run(collector.run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Collector service stopped.")