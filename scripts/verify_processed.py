import pandas as pd
import pyarrow.parquet as pq
import os
import logging

# Configuration
processed_path = "data/processed/2025/yellow/"
log_path = "logs/yellow/"
months = ["2025-01", "2025-02", "2025-03"]

# Expected columns after transformation
expected_columns = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "tip_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "Airport_fee", "cbd_congestion_fee", "Borough"
]

# Create log directory
os.makedirs(log_path, exist_ok=True)

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_file = os.path.join(log_path, "verify_processed.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

for month in months:
    input_file = f"{processed_path}/yellow_tripdata_{month}_cleaned.parquet"

    try:
        if not os.path.exists(input_file):
            logger.warning(f"{input_file} not found, skipping")
            continue

        logger.info(f"Verifying {input_file}")
        df = pd.read_parquet(input_file)

        # Check row count
        row_count = len(df)
        logger.info(f"Rows in {month}: {row_count}")

        # Check columns
        columns = df.columns.tolist()
        missing_cols = [col for col in expected_columns if col not in columns]
        if missing_cols:
            logger.warning(f"Missing columns: {missing_cols}")
        else:
            logger.info("All expected columns present")

        # Check data types
        expected_dtypes = {
            "VendorID": "int64",
            "tpep_pickup_datetime": "datetime64[ns]",
            "tpep_dropoff_datetime": "datetime64[ns]",
            "passenger_count": "float64",
            "trip_distance": "float64",
            "RatecodeID": "float64",
            "PULocationID": "int64",
            "DOLocationID": "int64",
            "payment_type": "int64",
            "fare_amount": "float64",
            "tip_amount": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "congestion_surcharge": "float64",
            "Airport_fee": "float64",
            "cbd_congestion_fee": "float64",
            "Borough": "object"
        }
        dtype_mismatch = {}
        for col, exp_dtype in expected_dtypes.items():
            if col in df.columns and str(df[col].dtype) != exp_dtype:
                dtype_mismatch[col] = {
                    "expected": exp_dtype, "actual": str(df[col].dtype)}
        if dtype_mismatch:
            logger.warning(f"Data type mismatches: {dtype_mismatch}")
        else:
            logger.info("All data types match expected")

        # Check for nulls
        null_counts = df.isnull().sum()
        if null_counts.any():
            logger.warning(f"Null values per column: {null_counts.to_dict()}")
        else:
            logger.info("No null values found")

        # Log sample
        logger.info(f"First 5 rows in {month}:\n{df.head().to_string()}")

    except Exception as e:
        logger.error(f"Error verifying {input_file}: {e}", exc_info=True)

logger.removeHandler(file_handler)
file_handler.close()
