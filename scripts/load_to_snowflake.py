import pandas as pd
import snowflake.connector
import os
import logging
from dotenv import load_dotenv

# Configuration
load_dotenv()
processed_path = "data/processed/2025/yellow/"
log_path = "logs/yellow/"
months = ["2025-01", "2025-02", "2025-03"]

# Snowflake configuration
snowflake_config = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": "TLC_DATA",
    "schema": "PUBLIC",
    "insecure_mode": True
}
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "yellow_trips_2025")
CHUNK_SIZE = 100000  # Process 100,000 rows per chunk

# Create log directory
os.makedirs(log_path, exist_ok=True)

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_file = os.path.join(log_path, "load_to_snowflake.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

try:
    # Connect to Snowflake
    conn = snowflake.connector.connect(**snowflake_config)
    logger.info("Connected to Snowflake")

    # Truncate table to remove existing data
    truncate_table_query = f"""
    TRUNCATE TABLE {SNOWFLAKE_TABLE}
    """
    conn.cursor().execute(truncate_table_query)
    logger.info(f"Existing data in {SNOWFLAKE_TABLE} removed")

    # Create table if not exists
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        VendorID INTEGER,
        tpep_pickup_datetime TIMESTAMP_NTZ,
        tpep_dropoff_datetime TIMESTAMP_NTZ,
        passenger_count FLOAT,
        trip_distance FLOAT,
        RatecodeID FLOAT,
        PULocationID INTEGER,
        DOLocationID INTEGER,
        payment_type INTEGER,
        fare_amount FLOAT,
        tip_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        congestion_surcharge FLOAT,
        Airport_fee FLOAT,
        cbd_congestion_fee FLOAT,
        Borough VARCHAR
    )
    """
    conn.cursor().execute(create_table_query)
    logger.info(f"Table {SNOWFLAKE_TABLE} created or verified")

    # Load each file in chunks
    for month in months:
        input_file = f"{processed_path}/yellow_tripdata_{month}_cleaned.parquet"
        if not os.path.exists(input_file):
            logger.warning(f"{input_file} not found, skipping")
            continue

        logger.info(f"Loading {input_file} into {SNOWFLAKE_TABLE}")
        df = pd.read_parquet(input_file)

        # Convert datetime columns to ISO strings
        df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].astype(str)
        df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].astype(str)

        # Prepare and execute chunked INSERT statements
        cursor = conn.cursor()
        sql = f"INSERT INTO {SNOWFLAKE_TABLE} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
        data_chunks = [df[i:i + CHUNK_SIZE].values.tolist()
                       for i in range(0, len(df), CHUNK_SIZE)]
        total_rows = 0

        for i, chunk in enumerate(data_chunks):
            cursor.executemany(sql, chunk)
            conn.commit()
            rows_in_chunk = len(chunk)
            total_rows += rows_in_chunk
            logger.info(
                f"Loaded chunk {i + 1} of {len(data_chunks)} for {month}: {rows_in_chunk} rows")

        logger.info(
            f"Loaded {total_rows} rows from {input_file} into {SNOWFLAKE_TABLE}")

    logger.info("Data load completed")

except Exception as e:
    logger.error(f"Error loading data: {e}", exc_info=True)
finally:
    if 'conn' in locals():
        conn.close()
        logger.info("Snowflake connection closed")

logger.removeHandler(file_handler)
file_handler.close()
