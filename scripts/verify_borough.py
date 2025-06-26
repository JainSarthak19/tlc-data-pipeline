import pandas as pd

# For January
df = pd.read_parquet(
    "data/processed/2025/yellow/yellow_tripdata_2025-01_cleaned.parquet")
print("January Borough Counts:")
print(df['Borough'].value_counts())
print("\n")

# For February
df = pd.read_parquet(
    "data/processed/2025/yellow/yellow_tripdata_2025-02_cleaned.parquet")
print("February Borough Counts:")
print(df['Borough'].value_counts())
print("\n")

# For March
df = pd.read_parquet(
    "data/processed/2025/yellow/yellow_tripdata_2025-03_cleaned.parquet")
print("March Borough Counts:")
print(df['Borough'].value_counts())
