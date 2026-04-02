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
