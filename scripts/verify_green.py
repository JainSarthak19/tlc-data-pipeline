import pyarrow.parquet as pq
import pandas as pd
import os
import logging

# Configuration for local processing
data_path = "data/raw/2025/green/"
lookup_path = "data/raw/2025/lookup/taxi_zone_lookup.csv"
log_path = "logs/green/"
dataset = "green"
months = ["2025-01", "2025-02", "2025-03"]

# Expected columns for Green (removed Airport_fee based on log)
expected_columns = [
    "lpep_pickup_datetime", "lpep_dropoff_datetime", "trip_distance",
    "fare_amount", "total_amount", "PULocationID", "DOLocationID",
    "congestion_surcharge", "tip_amount", "payment_type", "RatecodeID"
]

# Create log directory
os.makedirs(log_path, exist_ok=True)

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load taxi zone lookup
try:
    lookup = pd.read_csv(lookup_path)
    logger.info(f"Taxi zone lookup loaded: {lookup.columns.tolist()}")
except Exception as e:
    logger.error(f"Failed to load {lookup_path}: {e}", exc_info=True)
    exit()

# Process Green files
for month in months:
    log_file = os.path.join(log_path, f"{dataset}_{month}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    file_path = f"{data_path}/{dataset}_tripdata_{month}.parquet"
    try:
        if not os.path.exists(file_path):
            logger.warning(f"{file_path}: Not found")
            raise Exception(f"{file_path} missing")

        logger.info(f"Processing {file_path}")
        table = pq.read_table(file_path)
        columns = table.column_names
        logger.info(f"{file_path}: {table.num_rows} rows")
        logger.info(f"Columns: {columns}")

        missing_cols = [col for col in expected_columns if col not in columns]
        if missing_cols:
            logger.warning(f"Missing columns: {missing_cols}")
        else:
            logger.info("All expected columns present")

        select_cols = [
            "PULocationID", "tip_amount", "payment_type",
            "lpep_pickup_datetime", "trip_distance", "lpep_dropoff_datetime",
            "fare_amount"
        ]
        select_cols = [col for col in select_cols if col in columns]
        if not select_cols:
            logger.error("No columns available")
            raise Exception("No columns available")

        df = pq.read_table(file_path, columns=select_cols).to_pandas()

        if "PULocationID" in df.columns:
            null_pu = df["PULocationID"].isnull().sum()
            invalid_pu = len(df[~df["PULocationID"].isin(range(1, 266))])
            logger.info(f"Null PULocationID: {null_pu}")
            logger.info(f"Invalid PULocationID (not in 1-265): {invalid_pu}")

        for col in select_cols:
            if col != "PULocationID":
                null_count = df[col].isnull().sum()
                logger.info(f"Null {col}: {null_count}")

        if "tip_amount" in df.columns:
            high_tip = len(df[df["tip_amount"] > 100])
            logger.info(f"tip_amount > $100: {high_tip}")
            if high_tip > 0:
                sample_tips = df[df["tip_amount"] > 100][[
                    "tip_amount", "payment_type", "PULocationID", "trip_distance"]].head(2)
                logger.info(
                    f"Sample rows with tip_amount > $100:\n{sample_tips.to_string(index=False)}")

        if "trip_distance" in df.columns:
            high_distance = len(df[df["trip_distance"] > 100])
            logger.info(f"trip_distance > 100 miles: {high_distance}")
            if high_distance > 0:
                sample_distance = df[df["trip_distance"] > 100][[
                    "trip_distance", "PULocationID", "lpep_pickup_datetime"]].head(2)
                logger.info(
                    f"Sample rows with trip_distance > 100 miles:\n{sample_distance.to_string(index=False)}")

        if "lpep_pickup_datetime" in df.columns and "lpep_dropoff_datetime" in df.columns:
            df["trip_duration"] = (
                pd.to_datetime(df["lpep_dropoff_datetime"]) -
                pd.to_datetime(df["lpep_pickup_datetime"])
            ).dt.total_seconds() / 60
            neg_duration = len(df[df["trip_duration"] < 0])
            logger.info(f"Negative trip durations (minutes): {neg_duration}")
            if neg_duration > 0:
                sample_neg = df[df["trip_duration"] < 0][[
                    "lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID"]].head(2)
                logger.info(
                    f"Sample rows with negative trip durations:\n{sample_neg.to_string(index=False)}")

        if "payment_type" in df.columns:
            payment_dist = df["payment_type"].value_counts().to_string()
            logger.info(
                f"Payment type distribution (0=Flex Fare, 1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided):\n{payment_dist}")

        if "tip_amount" in df.columns and "payment_type" in df.columns:
            zero_tips_credit = len(
                df[(df["payment_type"] == 1) & (df["tip_amount"] == 0)])
            zero_tips_cash = len(
                df[(df["payment_type"] == 2) & (df["tip_amount"] == 0)])
            logger.info(
                f"tip_amount == 0 for payment_type=1 (credit): {zero_tips_credit}")
            logger.info(
                f"tip_amount == 0 for payment_type=2 (cash): {zero_tips_cash}")

            pt0_tips = df[df["payment_type"] == 0]["tip_amount"].agg(
                ["mean", "sum", "count"]).round(2)
            logger.info(
                f"tip_amount stats for payment_type=0 (Flex Fare):\n{pt0_tips.to_string()}")

        if "tip_amount" in df.columns and "PULocationID" in df.columns and "payment_type" in df.columns:
            df_tips = df[df["payment_type"].isin(
                [1, 2])][["PULocationID", "tip_amount"]].copy()
            df_with_borough = df_tips.merge(
                lookup[["LocationID", "Borough"]],
                left_on="PULocationID",
                right_on="LocationID",
                how="left"
            )
            tip_stats = df_with_borough.groupby("Borough")["tip_amount"].agg(
                ["mean", "sum", "count"]
            ).round(2)
            logger.info(
                f"Tip analysis by borough (credit and cash):\n{tip_stats.to_string()}")

            unknown_ids = df_with_borough[df_with_borough["Borough"].isna(
            )]["PULocationID"].unique().tolist()
            if unknown_ids:
                logger.info(f"Unknown borough PULocationIDs: {unknown_ids}")
            else:
                logger.info("No Unknown borough PULocationIDs found")

        if "tip_amount" in df.columns and "fare_amount" in df.columns and "PULocationID" in df.columns and "payment_type" in df.columns:
            df_tips = df[df["payment_type"].isin(
                [1, 2])][["PULocationID", "tip_amount", "fare_amount"]].copy()
            df_tips["tip_percentage"] = (
                df_tips["tip_amount"] / df_tips["fare_amount"].where(df_tips["fare_amount"] > 0)) * 100
            df_with_borough = df_tips.merge(
                lookup[["LocationID", "Borough"]],
                left_on="PULocationID",
                right_on="LocationID",
                how="left"
            )
            tip_pct_stats = df_with_borough.groupby("Borough")["tip_percentage"].agg(
                ["mean", "count"]
            ).round(2)
            logger.info(
                f"Tip percentage by borough (credit and cash):\n{tip_pct_stats.to_string()}")

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}", exc_info=True)
        print(f"Checks completed for {month} (error occurred)")

    finally:
        logger.removeHandler(file_handler)
        file_handler.close()
        print(f"Checks completed for {month}")
