# TLC Data Pipeline

NYC Taxi Fleet Optimization using TLC Trip Record Data (Yellow, Green, FHV, HVFHV) for Q1 2025.

## Overview
- **Objective**: Optimize NYC taxi fleet operations by analyzing demand, congestion, shared rides, pricing, wheelchair accessibility, airport efficiency, and tip revenue.
- **Data**: ~1.62 GB, ~10.8M rows for Jan–Mar 2025 (Yellow, Green, FHV, HVFHV).
- **Tools**: Amazon S3, Apache Airflow, Snowflake, Power BI, Python (PyArrow, Boto3).
- **Pipeline**: Download Parquet files, store in S3, process via Airflow, load to Snowflake, visualize in Power BI.
- **Business Requirements**:
  1. Maximize fleet utilization during peak demand.
  2. Reduce congestion delays.
  3. Improve shared ride efficiency (HVFHV).
  4. Optimize pricing strategies.
  5. Enhance wheelchair accessibility.
  6. Enhance airport trip efficiency.
  7. Maximize driver tip revenue by targeting high-tip boroughs.

## Folder Structure
- `data/`: Raw Parquet files, transformed data, lookup CSV.
- `scripts/`: Python scripts for data verification, ETL.
- `airflow/`: Airflow DAGs and Docker configuration.
- `powerbi/`: Power BI dashboard file.
- `config/`: AWS and Snowflake credentials (not tracked).
- `docs/`: Additional documentation (e.g., setup instructions).

## Getting Started
See `docs/SETUP.md` for setup instructions, including Conda environment, Git, and running verification scripts.

## License
MIT License (pending).