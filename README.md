# Portfolio-Pipeline

Project Structure:

Portfolio-Pipeline/

    docker-compose.yml          | Defines all services — Airflow, MinIO, Postgres, Streamlit
    
    requirements-dbt.txt        | Python dependencies installed
    
    requirements-airflow.txt    |
    
    requirements-dashboard.txt  | 
    
    .env                        | Secret values kept out of source control: MinIO credentials, Airflow Fernet key, Postgres password. Referenced by docker-compose.yml via ${VAR} syntax.
    
    README.md

    dags/
        stock_pipeline.py               | Main Airflow DAG. Defines one PythonOperator per ticker (AAPL, MSFT, etc.) that calls yfinance, validates the data, and writes Parquet to MinIO. A final BashOperator runs dbt after all fetches succeed.

    stock_transforms/
        dbt_project.yml                 | dbt project config: project name, model paths, materialisation defaults (views for staging, tables for marts), and target database (DuckDB).
        profiles.yml                    | dbt connection profile pointing to DuckDB with the httpfs extension enabled so DuckDB can read Parquet files directly from MinIO over the S3 protocol.

        macros/
            minio_secrets.sql           | dbt macro that runs the DuckDB SET statements to configure S3 credentials (endpoint, access key, secret) before any model reads from MinIO.
        models/

            staging/
                stg_prices.sql          | Staging view. Reads raw Parquet files from MinIO using read_parquet() with hive partitioning. Casts columns to correct types, drops nulls and zero-volume rows
                schema.yml              | dbt schema file for the staging layer. Declares not_null and accepted_range tests on ticker, date, close, and volume columns. Run with dbt test.

            intermediate/
                int_moving_averages.sql | Intermediate table. Computes SMA (7/21/50-day), EMA (12/26-day), daily returns, 30-day annualised volatility, true range, and ATR-14 using SQL window functions over stg_prices.

            marts/
                mart_dashboard.sql      | Final mart table consumed by Streamlit. Selects clean columns from int_moving_averages, adds a MACD column, rounds volatility and returns to display precision, and adds a bullish/bearish/neutral signal flag.

        tests/
            assert_no_future_dates.sql

    dashboard/
        app.py
        Dockerfile
    tests/
        test_ingestor.py



Portfolio-Pipeline is an end-to-end pipeline for generating stock technical indicators for a small set of Australian tickers.

Ingest (Airflow + Yahoo Finance → MinIO)

The Airflow DAG stock_price_pipeline downloads historical OHLCV data via yfinance for tickers:
LDX.AX, 4DX.AX, CU6.AX, PME.AX
It writes the data as Parquet into MinIO bucket stock-raw using this style of keys:
{ticker_sanitized}/year={YYYY}/month={MM}/{ticker_sanitized}_{YYYY-MM-DD}.parquet
Transform (dbt + DuckDB → transformed mart data inside DuckDB)

dbt uses the DuckDB adapter with the httpfs S3 extension so DuckDB can read Parquet directly from MinIO.
Main models:
stg_prices (staging view): reads from s3://stock-raw/*/year=*/month=*/*.parquet, casts types, dedupes by (ticker, date).
int_moving_averages (intermediate table): computes SMA/EMA, daily returns, volatility, true range/ATR, plus MACD/ATR-style aggregations via SQL windows.
mart_dashboard (mart table): selects final columns and derives a signal (“bullish/bearish/neutral”).
The dbt output is materialized as DuckDB tables in the database file configured in profiles.yml (/tmp/stock.duckdb), not automatically exported to S3 as Parquet.
Visualize (Streamlit + MinIO)

The Streamlit app is intended to load mart Parquet from MinIO bucket stock-transformed and plot indicators.
How to operate it (today, based on the code in your repo)
1) Start the stack
From Portfolio-Pipeline/:

docker compose up -d
Then check:

Airflow UI: http://localhost:8080
MinIO console: http://localhost:9001
Streamlit: http://localhost:8501
Credentials (from compose):

MinIO: minioadmin / minioadmin
Airflow user created at init: admin / admin
2) Run ingestion
In the Airflow UI, trigger the DAG:

stock_price_pipeline
That DAG runs the “fetch for each ticker → store Parquet into stock-raw” part.

3) Run dbt transforms
In current setup, run dbt manually in the dbt container:

docker compose exec dbt dbt run
Notes:

docker compose exec dbt dbt test currently fails because your staging schema uses dbt_utils.accepted_range, but dbt_utils isn’t installed in the dbt container requirements.
4) Use the dashboard
Open:

http://localhost:8501
Important: with the current repo state, the Streamlit dashboard is likely to not show data because:

dbt does not export Parquet into stock-transformed/mart_dashboard/ (it only builds tables inside DuckDB).
dashboard/app.py has a bug: it calls load_data(ticker) but the function defined is load_transformed_data(...).
So to “operate” the dashboard successfully, you’ll either need:

an export step from DuckDB to stock-transformed Parquet, and/or
to update the Streamlit app to query the DuckDB database directly (or whatever persistent store you choose).
Files to look at (core logic)
Ingestion DAG: dags/stock_pipeline.py
dbt config: stock_transforms/dbt_project.yml, stock_transforms/profiles.yml, stock_transforms/macros/minio_secrets.sql
dbt models:
stock_transforms/models/staging/stg_prices.sql
stock_transforms/models/intermediate/int_moving_averages.sql
stock_transforms/models/marts/mart_dashboard.sql
Dashboard: dashboard/app.py