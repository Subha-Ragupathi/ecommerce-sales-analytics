"""
============================================================
FILE: azure/data_pipeline.py
PROJECT: E-Commerce Sales Analytics
AUTHOR: Subha Ragupathi
DESCRIPTION:
    Azure Data Pipeline — simulates an end-to-end data 
    ingestion and transformation workflow using:
    - Azure Blob Storage (data lake landing zone)
    - Azure SQL Database (data warehouse)
    - pandas for transformation (replace with PySpark for scale)

ARCHITECTURE:
    Raw CSV → Azure Blob (Bronze) → Transform → Azure SQL (Silver/Gold)

PREREQUISITES:
    pip install azure-storage-blob azure-identity pyodbc pandas sqlalchemy

ENVIRONMENT VARIABLES REQUIRED:
    AZURE_STORAGE_CONNECTION_STRING
    AZURE_SQL_SERVER
    AZURE_SQL_DATABASE
    AZURE_SQL_USERNAME
    AZURE_SQL_PASSWORD
============================================================
"""

import os
import io
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# --- Conditional Azure imports (graceful fallback for local dev) ---
try:
    from azure.storage.blob import BlobServiceClient
    from azure.identity import DefaultAzureCredential
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    print("⚠️  Azure SDK not installed. Running in LOCAL SIMULATION mode.")

try:
    from sqlalchemy import create_engine, text
    SQL_AVAILABLE = True
except ImportError:
    SQL_AVAILABLE = False

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
STORAGE_CONN_STR   = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "PLACEHOLDER")
CONTAINER_BRONZE   = "bronze"       # Raw / landing zone
CONTAINER_SILVER   = "silver"       # Cleaned / transformed
BLOB_NAME_RAW      = "ecommerce/sales/ecommerce_sales_raw.csv"
BLOB_NAME_CLEAN    = "ecommerce/sales/ecommerce_sales_cleaned.csv"

SQL_SERVER         = os.getenv("AZURE_SQL_SERVER",   "your-server.database.windows.net")
SQL_DATABASE       = os.getenv("AZURE_SQL_DATABASE", "ecommerce_db")
SQL_USERNAME       = os.getenv("AZURE_SQL_USERNAME", "sqladmin")
SQL_PASSWORD       = os.getenv("AZURE_SQL_PASSWORD", "PLACEHOLDER")
SQL_TABLE          = "fact_orders_staging"

LOCAL_RAW_PATH     = Path("../data/raw/ecommerce_sales_raw.csv")
LOCAL_CLEAN_PATH   = Path("../data/processed/ecommerce_sales_cleaned.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# STEP 1 — EXTRACT: Upload raw file to Azure Blob (Bronze)
# ─────────────────────────────────────────────
def upload_to_bronze(local_path: Path) -> bool:
    """Upload raw CSV to Azure Blob Storage — Bronze layer."""
    log.info("STEP 1 ▶ Uploading raw data to Azure Blob (Bronze layer)")

    if not AZURE_AVAILABLE:
        log.info(f"  [SIMULATION] Would upload {local_path} → {CONTAINER_BRONZE}/{BLOB_NAME_RAW}")
        return True

    try:
        client       = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        container    = client.get_container_client(CONTAINER_BRONZE)
        blob_client  = container.get_blob_client(BLOB_NAME_RAW)

        with open(local_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)
            file_size = local_path.stat().st_size / 1024
            log.info(f"  ✅ Uploaded {local_path.name} ({file_size:.1f} KB) → {CONTAINER_BRONZE}/{BLOB_NAME_RAW}")
        return True
    except Exception as e:
        log.error(f"  ❌ Upload failed: {e}")
        return False


# ─────────────────────────────────────────────
# STEP 2 — TRANSFORM: Clean and enrich the data
# ─────────────────────────────────────────────
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply cleaning, validation, and feature engineering."""
    log.info("STEP 2 ▶ Transforming data (Silver layer)")

    original_count = len(df)

    # Parse dates
    df['order_date'] = pd.to_datetime(df['order_date'])

    # Remove duplicates
    df.drop_duplicates(subset='order_id', inplace=True)
    log.info(f"  → Removed {original_count - len(df)} duplicate rows")

    # Drop rows with critical nulls
    critical_cols = ['order_id', 'customer_id', 'total_amount', 'order_date']
    before = len(df)
    df.dropna(subset=critical_cols, inplace=True)
    log.info(f"  → Removed {before - len(df)} rows with critical null values")

    # Validate data ranges
    df = df[df['unit_price'] > 0]
    df = df[df['quantity'] > 0]
    df = df[df['discount_pct'].between(0, 100)]
    df = df[df['customer_rating'].between(1.0, 5.0)]

    # Feature engineering
    df['year']           = df['order_date'].dt.year
    df['month']          = df['order_date'].dt.month
    df['quarter']        = df['order_date'].dt.quarter
    df['month_name']     = df['order_date'].dt.strftime('%b')
    df['day_of_week']    = df['order_date'].dt.day_name()
    df['is_weekend']     = df['day_of_week'].isin(['Saturday', 'Sunday'])
    df['is_q4']          = df['month'].isin([10, 11, 12])
    df['profit']         = (df['revenue'] * 0.35).round(2)
    df['revenue_bucket'] = pd.cut(
        df['total_amount'],
        bins=[0, 50, 200, 500, 1000, 999999],
        labels=['<$50', '$50-200', '$200-500', '$500-1K', '>$1K']
    ).astype(str)

    # Standardise string columns
    str_cols = ['customer_segment', 'region', 'sales_channel',
                'category', 'product_name', 'payment_method', 'order_status']
    for col in str_cols:
        df[col] = df[col].str.strip().str.title()

    # Audit columns
    df['pipeline_run_ts'] = datetime.utcnow().isoformat()
    df['data_source']     = 'ecommerce_sales_raw_csv'

    log.info(f"  ✅ Transform complete → {len(df):,} clean rows ({original_count - len(df)} removed)")
    return df


# ─────────────────────────────────────────────
# STEP 3 — LOAD: Write cleaned data to Silver blob & Azure SQL
# ─────────────────────────────────────────────
def upload_to_silver(df: pd.DataFrame) -> bool:
    """Upload cleaned data to Azure Blob Storage — Silver layer."""
    log.info("STEP 3 ▶ Uploading cleaned data to Azure Blob (Silver layer)")

    # Always save locally
    df.to_csv(LOCAL_CLEAN_PATH, index=False)
    log.info(f"  ✅ Saved locally → {LOCAL_CLEAN_PATH}")

    if not AZURE_AVAILABLE:
        log.info(f"  [SIMULATION] Would upload to {CONTAINER_SILVER}/{BLOB_NAME_CLEAN}")
        return True

    try:
        client      = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        container   = client.get_container_client(CONTAINER_SILVER)
        blob_client = container.get_blob_client(BLOB_NAME_CLEAN)

        output = io.StringIO()
        df.to_csv(output, index=False)
        blob_client.upload_blob(output.getvalue().encode(), overwrite=True)
        log.info(f"  ✅ Uploaded to {CONTAINER_SILVER}/{BLOB_NAME_CLEAN}")
        return True
    except Exception as e:
        log.error(f"  ❌ Silver upload failed: {e}")
        return False


def load_to_azure_sql(df: pd.DataFrame) -> bool:
    """Load Gold-layer aggregates to Azure SQL Database."""
    log.info("STEP 4 ▶ Loading aggregated data to Azure SQL (Gold layer)")

    if not SQL_AVAILABLE:
        log.info("  [SIMULATION] SQLAlchemy not available — skipping SQL load")
        return True

    if SQL_PASSWORD == "PLACEHOLDER":
        log.info("  [SIMULATION] Azure SQL credentials not set — skipping SQL load")
        # Show what we WOULD load
        gold = df.groupby(['category', 'year', 'quarter']).agg(
            total_revenue=('total_amount', 'sum'),
            total_orders=('order_id', 'count'),
            avg_order_value=('total_amount', 'mean'),
            avg_rating=('customer_rating', 'mean')
        ).round(2).reset_index()
        log.info(f"  [SIMULATION] Would load {len(gold)} rows to {SQL_TABLE}")
        log.info(f"\n{gold.head(5).to_string()}")
        return True

    try:
        conn_str = (
            f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}"
            f"@{SQL_SERVER}/{SQL_DATABASE}"
            "?driver=ODBC+Driver+18+for+SQL+Server"
        )
        engine = create_engine(conn_str, fast_executemany=True)

        # Gold layer aggregate
        gold_df = df.groupby(['category', 'year', 'quarter']).agg(
            total_revenue=('total_amount', 'sum'),
            total_orders=('order_id', 'count'),
            avg_order_value=('total_amount', 'mean'),
            avg_rating=('customer_rating', 'mean')
        ).round(2).reset_index()
        gold_df['pipeline_run_ts'] = datetime.utcnow()

        gold_df.to_sql(SQL_TABLE, engine, if_exists='replace', index=False, chunksize=500)
        log.info(f"  ✅ Loaded {len(gold_df):,} rows to Azure SQL → {SQL_TABLE}")
        return True
    except Exception as e:
        log.error(f"  ❌ SQL load failed: {e}")
        return False


# ─────────────────────────────────────────────
# PIPELINE ORCHESTRATOR
# ─────────────────────────────────────────────
def run_pipeline():
    """Orchestrate the full ETL pipeline."""
    start_time = datetime.utcnow()
    log.info("=" * 60)
    log.info("  🚀 E-Commerce Sales Analytics Pipeline STARTED")
    log.info(f"  Run time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    log.info("=" * 60)

    # Step 1: Extract — load local raw data & upload to Bronze
    log.info(f"\nLoading raw data from {LOCAL_RAW_PATH}")
    df_raw = pd.read_csv(LOCAL_RAW_PATH)
    log.info(f"  → Raw dataset: {df_raw.shape[0]:,} rows × {df_raw.shape[1]} columns")
    upload_to_bronze(LOCAL_RAW_PATH)

    # Step 2: Transform
    df_clean = transform_data(df_raw.copy())

    # Step 3: Load to Silver (Blob)
    upload_to_silver(df_clean)

    # Step 4: Load to Gold (Azure SQL)
    load_to_azure_sql(df_clean)

    # Summary
    elapsed = (datetime.utcnow() - start_time).total_seconds()
    log.info("\n" + "=" * 60)
    log.info("  ✅ Pipeline COMPLETED SUCCESSFULLY")
    log.info(f"  Total time: {elapsed:.2f} seconds")
    log.info(f"  Rows processed: {len(df_clean):,}")
    log.info("=" * 60)

    return df_clean


if __name__ == "__main__":
    df_result = run_pipeline()
    print(f"\nSample output:\n{df_result[['order_id','category','total_amount','profit','year']].head(5)}")
