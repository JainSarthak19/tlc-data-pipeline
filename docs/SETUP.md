# TLC Data Pipeline Setup Instructions

## Prerequisites
- **Hardware**: Laptop with 16 GB RAM, SSD, ~3 GB free storage for Q1 2025 data (~1.62 GB).
- **Software**:
  - Miniconda or Anaconda (Python 3.11)
  - Git
  - AWS CLI (for S3 setup in Step 2)
  - Docker (for Airflow in Step 4)
  - Power BI Desktop (for visualization in Step 8)
- **Accounts**:
  - AWS (for S3 bucket)
  - Snowflake (for data warehouse)
  - GitHub (for version control)

## Setup Steps

### 1. Clone Repository
```bash
git clone <your-remote-url>  # e.g., https://github.com/your-username/tlc-data-pipeline.git
cd tlc-data-pipeline
```

### 2. Set Up Conda Environment
```bash
conda create -n tlc-pipeline python=3.11
conda activate tlc-pipeline
pip install -r requirements.txt
```

### 3. Move Data Files
Place Q1 2025 Parquet files (`yellow_tripdata_2025-01.parquet`, etc.) and `taxi_zone_lookup.csv` in:
- `data/raw/2025/yellow/`
- `data/raw/2025/green/`
- `data/raw/2025/fhv/`
- `data/raw/2025/hvfhv/`
- `data/raw/2025/lookup/`

### 4. Verify Data
Run the verification script to check columns, discrepancies, and tip insights:
```bash
python scripts/verify_tlc_data.py
```

### 5. Next Steps
- Set up S3 bucket (`s3://nyc-tlc-data-2025`) and upload data (Step 2).
- Configure Airflow for ETL orchestration (Step 4).
- Load transformed data to Snowflake (Step 7).
- Build Power BI dashboards with metrics like Demand Score and Tip Score (Step 8).

## Notes
- Data files are excluded from Git via `.gitignore` due to size (~1.62 GB).
- Secure credentials in `config/` (e.g., `aws_credentials.yaml`) are not tracked.
- See `README.md` for project overview and business requirements.
