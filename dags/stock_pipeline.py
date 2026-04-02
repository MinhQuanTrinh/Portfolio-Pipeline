from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta   
import logging

log = logging.getLogger(__name__)

TICKERS = ["LDX.AX", "4DX.AX", "CU6.AX", "PME.AX"]
MINIO_ENDPOINT = "http://minio:9000"
RAW_BUCKET = "stock-raw"


def fetch_and_store(ticker: str, ds: str, **kwargs):
    """"Fetch data and write Parquet to MinIO raw bucket."""
    import yfinance as yf
    import pandas as pd
    import boto3
    import io

    log.info(f"Fetching data for {ticker} up to {ds}")

    df = yf.download(
        ticker,
        start="2026-01-01",
        end=ds,
        auto_adjust=True,
        progress=False,
    )

    if df.empty:
        log.warning(f"No data found for {ticker} on {ds}")
        return
    
    # Flatten MultiIndex columns if present
    df.columns = [c[0].lower() if isinstance(c, tuple) else c.lower() for c in df.columns]
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date", "Date": "date"}, inplace=True)
    df["ticker"] = ticker
    df["ingested_at"] = datetime.utcnow().isoformat()

    #validate - drop rows with null close.
    original_len = len(df)
    df = df.dropna(subset=["close"])
    if len(df) < original_len:
        log.warning(f"Dropped {original_len - len(df)} rows with null close for {ticker} on {ds}")

    #write to parquet in memory
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    #upload to MinIO
    s3 = boto3.client(
        "s3", 
        endpoint_url=MINIO_ENDPOINT, 
        aws_access_key_id="minioadmin", 
        aws_secret_access_key="minioadmin")

    key = f"{ticker}/year={ds[:4]}/month={ds[5:7]}/{ticker}_{ds}.parquet"
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=buf.getvalue())
    log.info(f"✓ Stored {len(df)} rows → s3://{RAW_BUCKET}/{key}")

with DAG(
    dag_id ="stock_price_pipeline",
    description="Fetch and store stock prices from Yahoo Finance to MinIO",
    schedule_interval="0 18 * * 1-5",
    start_date=datetime(2026, 1, 1),
    end_date=datetime(2026, 1, 31),
    catchup=False,
    tags=["finance", "ingestion"],
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
        "email": ["trinhminhquan2810@gmail.com"],
        "email_on_success": True,
    },
) as dag:
    for _ticker in TICKERS:
        PythonOperator(
            task_id=f"fetch_{_ticker.lower()}",
            python_callable=fetch_and_store,
            op_kwargs={"ticker": _ticker, "ds": "{{ ds }}"},
        )