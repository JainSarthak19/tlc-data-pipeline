import pandas as pd
import pyarrow.parquet as pq
import os
import logging
import numpy as np
from dotenv import load_dotenv

# Configuration
load_dotenv()
raw_path = "data/raw/2025/yellow/"
processed_path = "data/processed/2025/yellow/"
lookup_path = "data/raw/2025/lookup/taxi_zone_lookup.csv"
log_path = "logs/yellow/"
months = ["2025-01", "2025-02", "2025-03"]
chunk_size = 100000

# Create directories
os.makedirs(processed_path, exist_ok=True)
os.makedirs(log_path, exist_ok=True)

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_file = os.path.join(log_path, "transform_yellow.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# Load taxi zone lookup
try:
    lookup = pd.read_csv(lookup_path)
    logger.info(f"Taxi zone lookup loaded: {lookup.columns.tolist()}")
    logger.info(
        f"Unique Boroughs in lookup: {lookup['Borough'].unique().tolist()}")
except Exception as e:
    logger.error(f"Failed to load {lookup_path}: {e}", exc_info=True)
    exit()

for month in months:
    input_file = f"{raw_path}/yellow_tripdata_{month}.parquet"
    output_file = f"{processed_path}/yellow_tripdata_{month}_cleaned.parquet"

    try:
        if not os.path.exists(input_file):
            logger.warning(f"{input_file} not found, skipping")
            continue

        logger.info(f"Transforming {input_file}")
        chunks = []
        total_rows = 0
        invalid_date_rows = 0
        dropped_rows = 0

        # Process in chunks
        for batch in pq.read_table(input_file).to_batches(max_chunksize=chunk_size):
            df_chunk = batch.to_pandas()

            # Convert and filter dates with explicit ns precision
            df_chunk['tpep_pickup_datetime'] = pd.to_datetime(
                df_chunk['tpep_pickup_datetime'], errors='coerce', unit='ns')
            df_chunk['tpep_dropoff_datetime'] = pd.to_datetime(
                df_chunk['tpep_dropoff_datetime'], errors='coerce', unit='ns')
            invalid_dates = df_chunk[df_chunk['tpep_pickup_datetime'].isna(
            ) | df_chunk['tpep_dropoff_datetime'].isna()]
            if not invalid_dates.empty:
                logger.warning(
                    f"Invalid dates in {month}: {len(invalid_dates)} rows")
                invalid_date_rows += len(invalid_dates)
            df_chunk = df_chunk.dropna(
                subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
            df_chunk = df_chunk[(df_chunk['tpep_pickup_datetime'].dt.year == 2025) &
                                (df_chunk['tpep_pickup_datetime'].dt.month == int(month.split('-')[1])) &
                                (df_chunk['tpep_dropoff_datetime'].dt.year == 2025) &
                                (df_chunk['tpep_dropoff_datetime'].dt.month == int(month.split('-')[1]))]

            # Handle nulls (drop rows with nulls in key fields tied to Flex Fare)
            df_chunk = df_chunk.dropna(subset=['passenger_count', 'RatecodeID', 'store_and_fwd_flag',
                                               'congestion_surcharge', 'Airport_fee'])

            # Filter outliers and invalid PULocationID
            df_chunk = df_chunk[df_chunk["tip_amount"] <= 100]
            df_chunk = df_chunk[df_chunk["trip_distance"] <= 100]
            df_chunk = df_chunk[df_chunk["tpep_dropoff_datetime"]
                                >= df_chunk["tpep_pickup_datetime"]]
            df_chunk = df_chunk[df_chunk["PULocationID"]
                                != 265]  # Exclude unknown zone

            # Exclude payment_type 3,4,5 and nulls
            df_chunk = df_chunk[~df_chunk["payment_type"].isin([3, 4, 5])]
            df_chunk = df_chunk[df_chunk["payment_type"].notnull()]

            # Join with lookup to add Borough with debugging
            df_chunk = df_chunk.merge(lookup[["LocationID", "Borough"]], left_on="PULocationID",
                                      right_on="LocationID", how="left").drop(columns=["LocationID"])
            unmatched_pu = df_chunk[df_chunk['Borough'].isna(
            )]['PULocationID'].unique()
            if len(unmatched_pu) > 0:
                logger.warning(
                    f"Unmatched PULocationIDs in {month}: {unmatched_pu.tolist()}")
                logger.info(
                    f"Sample rows with unmatched PULocationIDs:\n{df_chunk[df_chunk['PULocationID'].isin(unmatched_pu)].head().to_string()}")
            unknown_borough_count = df_chunk[df_chunk['Borough'].isna() | (
                df_chunk['Borough'] == '')].shape[0]
            if unknown_borough_count > 0:
                logger.warning(
                    f"Unknown Borough values in {month}: {unknown_borough_count} rows")

            # Calculate Airport_fee
            df_chunk['Airport_fee'] = np.where(
                df_chunk['PULocationID'].isin([1, 132]), 5.0, 0.0)

            # Drop unneeded columns
            df_chunk = df_chunk.drop(
                columns=['store_and_fwd_flag', 'mta_tax', 'extra', 'tolls_amount'])

            if not df_chunk.empty:
                chunks.append(df_chunk)
                total_rows += len(df_chunk)
                dropped_rows += len(batch.to_pandas()) - len(df_chunk)

        if chunks:
            df_final = pd.concat(chunks, ignore_index=True)

            # QA check
            null_counts = df_final.isnull().sum()
            if null_counts.any():
                logger.warning(
                    f"Null values per column in {month}: {null_counts.to_dict()}")
            valid_rows = df_final.dropna().shape[0]
            if total_rows != valid_rows:
                logger.warning(
                    f"Data quality issue in {month}: {total_rows - valid_rows} rows dropped due to nulls")

            # Log sample data
            if not df_final.empty:
                logger.info(
                    f"First 5 rows in {month}:\n{df_final.head().to_string()}")
                logger.info(
                    f"Last 5 rows in {month}:\n{df_final.tail().to_string()}")

            # Save cleaned data with explicit data types
            df_final = df_final.astype({
                "VendorID": "int64",
                "PULocationID": "int64",
                "DOLocationID": "int64",
                "tpep_pickup_datetime": "datetime64[ns]",
                "tpep_dropoff_datetime": "datetime64[ns]"
            })
            df_final.to_parquet(output_file, engine="pyarrow",
                                index=False, compression="snappy")
            logger.info(
                f"Saved {output_file}, total rows: {total_rows}, clean rows: {df_final.shape[0]}, dropped rows: {dropped_rows}, invalid date rows: {invalid_date_rows}")
        else:
            logger.warning(f"No data after filtering for {month}")

    except Exception as e:
        logger.error(f"Error transforming {input_file}: {e}", exc_info=True)

logger.removeHandler(file_handler)
file_handler.close()
