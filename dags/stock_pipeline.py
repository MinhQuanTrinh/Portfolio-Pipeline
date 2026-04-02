from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

TICKERS = ["LDX.AX", "4DX.AX", "CU6.AX", "PME.AX"]
MINIO_ENDPOINT = "http://minio:9000"
RAW_BUCKET = "stock-raw"


def fetch_and_store(ticker: str, ds: str, **kwargs):
    """Fetch data and write Parquet to MinIO raw bucket."""
    import yfinance as yf
    import pandas as pd
    import boto3, io, time
    from datetime import datetime

    log.info(f"Fetching data for {ticker} up to {ds}")

    # Retry up to 3 times with backoff
    df = None
    for attempt in range(3):
        try:
            df = yf.download(
                ticker,
                start="2020-01-01",
                end=ds,
                auto_adjust=True,
                progress=False,
            )
            if not df.empty:
                break
            log.warning(f"Empty result for {ticker}, attempt {attempt+1}/3")
            time.sleep(5 * (attempt + 1))
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed for {ticker}: {e}")
            time.sleep(5 * (attempt + 1))

    if df is None or df.empty:
        raise ValueError(f"No data returned for {ticker} after 3 attempts")

    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0].lower() for col in df.columns]
    else:
        df.columns = [col.lower() for col in df.columns]

    df.reset_index(inplace=True)
    df.rename(columns={"index": "date", "Date": "date"}, inplace=True)

    safe_ticker = ticker.replace(".", "_")
    df["ticker"] = ticker
    df["ingested_at"] = datetime.utcnow().isoformat()

    original_len = len(df)
    df = df.dropna(subset=["close"])
    if len(df) < original_len:
        log.warning(f"Dropped {original_len - len(df)} rows with null close for {ticker}")

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    key = f"{safe_ticker}/year={ds[:4]}/month={ds[5:7]}/{safe_ticker}_{ds}.parquet"
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=buf.getvalue())
    log.info(f"✓ Stored {len(df)} rows → s3://{RAW_BUCKET}/{key}")


with DAG(
    dag_id="stock_price_pipeline",
    description="Fetch and store stock prices from Yahoo Finance to MinIO",
    schedule_interval="0 8 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "ingestion"],
    default_args={
        "owner": "data-eng",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
    },
) as dag:
    for _ticker in TICKERS:
        PythonOperator(
            task_id=f"fetch_{_ticker.lower()}",
            python_callable=fetch_and_store,
            op_kwargs={"ticker": _ticker, "ds": "{{ ds }}"},
        )