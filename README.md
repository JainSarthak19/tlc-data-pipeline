# TLC Data Pipeline

NYC Taxi Fleet Optimization using TLC Trip Record Data (Yellow, Green, FHV, HVFHV) for Q1 2025.

## Overview
- **Objective**: Optimize NYC taxi fleet operations by analyzing demand, congestion, shared rides, pricing, wheelchair accessibility, airport efficiency, and driver tip revenue.
- **Data**: ~1.62 GB, ~10.8M rows for January–March 2025 across Yellow, Green, FHV, and HVFHV datasets.
- **Tools**: Amazon S3, Apache Airflow, Snowflake, Power BI Desktop, Python (PyArrow, Boto3, Snowflake-connector).
- **Pipeline**: Download Parquet files, store in S3 Glacier Instant Retrieval, process via Airflow, load to Snowflake, and visualize in Power BI.
- **Business Requirements**:
  1. Maximize fleet utilization during peak demand periods.
  2. Reduce operational costs by minimizing congestion delays.
  3. Improve shared ride efficiency for HVFHV operators.
  4. Optimize pricing strategies to balance demand and supply.
  5. Enhance accessibility for wheelchair users.
  6. Enhance airport trip efficiency.
  7. Maximize driver tip revenue by targeting high-tip boroughs.
- **Budget**: \$10–\$15 total for 27 months (~$0.36–$0.56/month).
- **Output**: Power BI dashboards with metrics like Demand Score, Congestion Score, Shared Ride Success, Revenue Score, WAV Fulfillment, Airport Efficiency, and Tip Score.

## Folder Structure
- `data/`: Raw Parquet files, transformed data, and taxi zone lookup CSV (not tracked in Git).
- `scripts/`: Python scripts for data verification, extraction, transformation, and loading.
- `airflow/`: Airflow DAGs and Docker configuration for ETL orchestration.
- `powerbi/`: Power BI dashboard file (not tracked).
- `config/`: AWS and Snowflake credentials (not tracked).
- `docs/`: Documentation, including setup instructions.
- `venv/`: Conda environment (not tracked).

## Getting Started
See \`docs/SETUP.md\` for detailed setup instructions, including Conda environment setup, Git configuration, and running the data verification script.

## License
MIT License (pending).

## Contact
For issues or contributions, open a GitHub issue or contact the project maintainer.