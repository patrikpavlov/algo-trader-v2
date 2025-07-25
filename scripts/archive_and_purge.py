import os
import sys
import logging
from datetime import date, timedelta

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
    """Loads configuration from environment variables with robust error checking."""
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
        "retention_days": int(os.getenv("RETENTION_DAYS", "7"))
    }

    if not all([config["db_user"], config["db_password"], config["db_name"]]):
        logger.critical("FATAL ERROR: One or more database credentials are missing from the .env.db file.")
        sys.exit(1)
        
    return config

def archive_and_purge_daily(table_name: str, engine, retention_days: int, archive_path: str):
    """
    Archives data older than retention_days to daily Parquet files and purges it.
    """
    cutoff_date = date.today() - timedelta(days=retention_days)
    logger.info(f"Processing table '{table_name}'. Archiving data older than {cutoff_date}.")

    table_archive_path = os.path.join(archive_path, table_name)
    os.makedirs(table_archive_path, exist_ok=True)

    # Determine the full date range to process once
    try:
        with engine.connect() as connection:
            min_date_result = connection.execute(text(f"SELECT min(time)::date FROM {table_name};")).scalar()
    except Exception as e:
        logger.critical(f"Could not connect to the database to determine date range. Aborting. Error: {e}")
        return

    if not min_date_result:
        logger.info(f"No data found in table '{table_name}'. Nothing to do.")
        return

    # Loop through each day that needs to be archived
    current_date = min_date_result
    while current_date < cutoff_date:
        day_start = current_date
        day_end = current_date + timedelta(days=1)
        parquet_file_path = os.path.join(table_archive_path, f"{day_start}.parquet")
        
        # We will now manage the connection and transaction explicitly for each day
        try:
            # 1. Read data for the day
            with engine.connect() as connection:
                extract_query = text(f"SELECT * FROM {table_name} WHERE time >= '{day_start}' AND time < '{day_end}'")
                df = pd.read_sql(extract_query, connection)

            # 2. If there's data, write it to Parquet first
            if not df.empty:
                df.to_parquet(parquet_file_path, index=False)
                logger.info(f"Saved {len(df)} rows to {parquet_file_path}.")

                # 3. Only if Parquet write is successful, connect again and delete
                with engine.connect() as connection:
                    with connection.begin(): # Explicitly begin a transaction
                        purge_query = text(f"DELETE FROM {table_name} WHERE time >= '{day_start}' AND time < '{day_end}'")
                        result = connection.execute(purge_query)
                        logger.info(f"✅ Successfully purged {result.rowcount} rows for {day_start} from '{table_name}'.")
            else:
                logger.info(f"No data for {day_start} in '{table_name}', skipping.")

        except Exception as day_processing_error:
            logger.error(f"Failed to process {day_start} for table '{table_name}'. Error: {day_processing_error}")
            # Clean up partial file if it exists
            if os.path.exists(parquet_file_path):
                logger.warning(f"Removing partial Parquet file to ensure a clean retry: {parquet_file_path}")
                os.remove(parquet_file_path)
        
        current_date += timedelta(days=1)

if __name__ == "__main__":
    config = load_config()
    db_uri = (f"postgresql://{config['db_user']}:{config['db_password']}@"
              f"{config['db_host']}:{config['db_port']}/{config['db_name']}")
    engine = create_engine(db_uri)

    tables_to_process = ["trades", "orderbook_snapshots"]
    for table in tables_to_process:
        archive_and_purge_daily(
            table_name=table,
            engine=engine,
            retention_days=config['retention_days'],
            archive_path=config['archive_path']
        )
    logger.info("Archiving process finished.")