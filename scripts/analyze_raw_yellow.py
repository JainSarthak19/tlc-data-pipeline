import pyarrow.parquet as pq
import pandas as pd
import os
import logging

# Configuration
raw_path = "data/raw/2025/yellow/"
log_path = "logs/yellow/"
dataset = "yellow"
months = ["2025-01", "2025-02", "2025-03"]

# Expected columns for raw Yellow data
expected_columns = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee",
    "cbd_congestion_fee"
]

# Create log directory
os.makedirs(log_path, exist_ok=True)

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Process each raw Yellow file
for month in months:
    # Configure file handler for this month
    log_file = os.path.join(log_path, f"raw_{dataset}_{month}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    file_path = f"{raw_path}/yellow_tripdata_{month}.parquet"
    if not os.path.exists(file_path):
        logger.warning(f"{file_path}: File not found")
        logger.removeHandler(file_handler)
        file_handler.close()
        print(f"Analysis completed for {month} (file not found)")
        continue

    try:
        logger.info(f"Analyzing {file_path}")
        # Read full dataset
        table = pq.read_table(file_path)
        df_full = table.to_pandas()
        columns = df_full.columns.tolist()
        logger.info(f"{file_path}: {len(df_full)} rows")
        logger.info(f"Columns: {columns}")

        # Verify expected columns
        missing_cols = [col for col in expected_columns if col not in columns]
        if missing_cols:
            logger.warning(f"Missing columns: {missing_cols}")
        else:
            logger.info("All expected columns present")

        # Null checks for all columns
        for col in expected_columns:
            if col in df_full.columns:
                null_count = df_full[col].isnull().sum()
                logger.info(f"Null {col} (full): {null_count}")
            else:
                logger.warning(f"{col} not found in full data")

        # Timestamp validation (full dataset) with debugging
        if "tpep_pickup_datetime" in df_full.columns and "tpep_dropoff_datetime" in df_full.columns:
            logger.info("Starting timestamp validation...")
            df_full["tpep_pickup_datetime"] = pd.to_datetime(
                df_full["tpep_pickup_datetime"], errors='coerce')
            df_full["tpep_dropoff_datetime"] = pd.to_datetime(
                df_full["tpep_dropoff_datetime"], errors='coerce')
            invalid_pickup = df_full[df_full["tpep_pickup_datetime"].isna()]
            invalid_dropoff = df_full[df_full["tpep_dropoff_datetime"].isna()]
            if not invalid_pickup.empty or not invalid_dropoff.empty:
                logger.warning(
                    f"Invalid pickup dates (full): {len(invalid_pickup)} rows")
                logger.warning(
                    f"Invalid dropoff dates (full): {len(invalid_dropoff)} rows")
                if not invalid_pickup.empty:
                    logger.info(
                        f"Sample invalid pickup dates:\n{invalid_pickup.head().to_string()}")
                if not invalid_dropoff.empty:
                    logger.info(
                        f"Sample invalid dropoff dates:\n{invalid_dropoff.head().to_string()}")
            else:
                logger.info("No invalid pickup or dropoff dates found")
        else:
            logger.warning("Timestamp columns missing, skipping validation")

        # Categorical value checks (full dataset)
        if "VendorID" in df_full.columns:
            valid_vendors = df_full["VendorID"].isin([1, 2, 6, 7]).sum()
            logger.info(
                f"Valid VendorIDs (1, 2, 6, 7) in full: {valid_vendors}/{len(df_full)}")
        if "payment_type" in df_full.columns:
            valid_payments = df_full["payment_type"].isin(
                [0, 1, 2, 3, 4, 5, 6]).sum()
            logger.info(
                f"Valid payment_types (0-6) in full: {valid_payments}/{len(df_full)}")
        if "RatecodeID" in df_full.columns:
            valid_rates = df_full["RatecodeID"].isin(
                [1, 2, 3, 4, 5, 6, 99]).sum()
            logger.info(
                f"Valid RatecodeIDs (1-6, 99) in full: {valid_rates}/{len(df_full)}")

        # Outlier checks (full dataset)
        if "trip_distance" in df_full.columns:
            high_distance = len(df_full[df_full["trip_distance"] > 100])
            logger.info(f"trip_distance > 100 miles in full: {high_distance}")
        if "fare_amount" in df_full.columns:
            high_fare = len(df_full[df_full["fare_amount"] > 1000])
            logger.info(f"fare_amount > $1000 in full: {high_fare}")

    except Exception as e:
        logger.error(f"Error analyzing {file_path}: {e}", exc_info=True)
        print(f"Analysis completed for {month} (error occurred)")

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()
        print(f"Analysis completed for {month}")
