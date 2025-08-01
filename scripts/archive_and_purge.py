# scripts/archive_and_purge.py
import os
import sys
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("archive_and_purge.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
def load_config():
    """Loads configuration from environment variables."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dotenv_path = os.path.join(script_dir, '..', '.env.db')

    if not os.path.exists(dotenv_path):
        logger.critical(f"FATAL ERROR: Environment file not found at the expected path: {dotenv_path}")
        sys.exit(1)

    load_dotenv(dotenv_path=dotenv_path)
    
    config = {
        "db_user": os.getenv("POSTGRES_USER"),
        "db_password": os.getenv("POSTGRES_PASSWORD"),
        "db_name": os.getenv("POSTGRES_DB"),
        "db_host": "localhost",
        "db_port": os.getenv("DB_PORT", "5432"),
        "archive_path": os.getenv("ARCHIVE_PATH", "cold_storage"),
        "retention_minutes": int(os.getenv("RETENTION_MINUTES", "15")) 
    }

    if not all([config["db_user"], config["db_password"], config["db_name"]]):
        logger.critical("FATAL ERROR: One or more database credentials are missing from the .env.db file.")
        sys.exit(1)
        
    return config

def archive_and_purge_chunks(table_name: str, engine, archive_path: str, retention_minutes: int):
    """
    Archives and purges TimescaleDB chunks that are older than the retention period.
    """
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=retention_minutes)
    logger.info(f"Processing hypertable '{table_name}'. Finding chunks with data older than {cutoff_time.isoformat()}.")
    
    table_archive_path = os.path.join(archive_path, table_name)
    os.makedirs(table_archive_path, exist_ok=True)

    find_chunks_query = text("""
        SELECT chunk_schema, chunk_name, range_start, range_end
        FROM timescaledb_information.chunks
        WHERE hypertable_name = :table_name AND range_end < :cutoff
        ORDER BY range_end ASC;
    """)

    try:
        with engine.connect() as connection:
            eligible_chunks = connection.execute(find_chunks_query, {"table_name": table_name, "cutoff": cutoff_time}).fetchall()

        if not eligible_chunks:
            logger.info(f"No chunks older than the retention period found for '{table_name}'.")
            return

        logger.info(f"Found {len(eligible_chunks)} chunks to process for '{table_name}'.")

        for chunk_info in eligible_chunks:
            chunk_schema, chunk_name, range_start, range_end = chunk_info
            full_chunk_name = f'"{chunk_schema}"."{chunk_name}"'
            
            logger.info(f"Processing chunk: {full_chunk_name} with time range {range_start} to {range_end}")
            parquet_file_path = None

            try:
                with engine.connect() as connection:
                    df = pd.read_sql(f"SELECT * FROM {full_chunk_name} ORDER BY time ASC", connection)

                if df.empty:
                    logger.warning(f"Chunk {full_chunk_name} is empty. Dropping it without archiving.")
                else:
                    start_time = pd.to_datetime(df['time'].iloc[0]).strftime('%Y%m%d-%H%M%S')
                    end_time = pd.to_datetime(df['time'].iloc[-1]).strftime('%Y%m%d-%H%M%S')
                    
                    filename = f"{table_name}_from_{start_time}_to_{end_time}.parquet"
                    parquet_file_path = os.path.join(table_archive_path, filename)

                    logger.info(f"Saving {len(df)} rows from {full_chunk_name} to {parquet_file_path}")
                    df.to_parquet(parquet_file_path, index=False)
                
                logger.info(f"Successfully processed chunk {full_chunk_name}. Now dropping it.")
                with engine.connect() as connection:
                    with connection.begin():
                        # CORRECTED: Use a simple f-string for the table name and named arguments for timestamps.
                        # This is safe because table_name comes from the database, not user input.
                        drop_query = text(f"SELECT drop_chunks('{table_name}', newer_than => :start_time, older_than => :end_time);")
                        connection.execute(drop_query, {
                            "start_time": range_start,
                            "end_time": range_end
                        })
                logger.info(f"✅ Successfully dropped chunk covering range {range_start} to {range_end}.")

            except Exception as e:
                logger.error(f"Failed to process chunk {full_chunk_name}. The chunk will NOT be dropped. Error: {e}", exc_info=True)
                if parquet_file_path and os.path.exists(parquet_file_path):
                    logger.warning(f"Removing partial Parquet file to ensure a clean retry: {parquet_file_path}")
                    os.remove(parquet_file_path)

    except Exception as e:
        logger.critical(f"A critical error occurred while fetching chunks for '{table_name}'. Aborting. Error: {e}")

if __name__ == "__main__":
    config = load_config()
    db_uri = (f"postgresql://{config['db_user']}:{config['db_password']}@"
              f"{config['db_host']}:{config['db_port']}/{config['db_name']}")
    engine = create_engine(db_uri)

    tables_to_process = ["trades", "orderbook_snapshots"]
    for table in tables_to_process:
        archive_and_purge_chunks(
            table_name=table,
            engine=engine,
            archive_path=config['archive_path'],
            retention_minutes=config['retention_minutes']
        )
    logger.info("Archiving and purging process finished.")