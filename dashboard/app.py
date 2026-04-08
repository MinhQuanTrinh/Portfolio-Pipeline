import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import boto3, io, os

st.set_page_config(page_title="Portfolio Dashboard", layout="wide", page_icon="📊")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
TRANSFORMED_BUCKET = "stock-transformed"

@st.cache_data(ttl=3600)
def load_transformed_data(ticker: str) -> pd.DataFrame:
    """Read mart Parquets directly from MinIO via DuckDB."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='us-east-1';
        SET s3_url_style='path';
        SET s3_endpoint='minio:9000';
        SET s3_use_ssl=false;
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
    """)
    df = con.execute(f"""
        SELECT * FROM read_parquet(
            's3://{TRANSFORMED_BUCKET}/mart_dashboard/*.parquet'
        )
        WHERE ticker = '{ticker}'
        ORDER BY date
    """).df()
    con.close()
    return df

# ── Sidebar
st.sidebar.title("Controls")
tickers = st.sidebar.selectbox("Select Ticker", ["LDX.AX", "4DX.AX", "PME.AX", "CU6.AX"])
ma_options = st.sidebar.multiselect(
    "Moving averages",
    ["SMA 7", "SMA 21", "SMA 50"],
    default=["SMA 21", "SMA 50"],
)
df = load_data(ticker)

if df.empty:
    st.warning("No data available for the selected ticker. Run the Pipeline fist")
    st.stop()

start_date, end_date = st.sidebar.select_slider(
    "Date range",
    options=sorted(df["date"].unique().astype(str)),
    value=(df["date"].astype(str).min(), df["date"].astype(str).max()),
)
df = df[(df["date"].astype(str) >= start_date) &
        (df["date"].astype(str) <= end_date)]

# ── KPI cards ────────────────────────────────────────────────────────
st.title(f"📈 {ticker} — Stock Analytics")

latest = df.iloc[-1]
prev   = df.iloc[-2]
delta  = latest["close"] - prev["close"]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Close",      f"${latest['close']:.2f}",        f"{delta:+.2f}")
col2.metric("Volume",     f"{latest['volume']/1e6:.1f}M")
col3.metric("Volatility", f"{latest['volatility_pct']:.1f}%")
col4.metric("Signal",     latest["signal"].capitalize())

# ── Price + Moving Average chart ─────────────────────────────────────
fig = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    row_heights=[0.55, 0.25, 0.20],
    vertical_spacing=0.04,
)

fig.add_trace(go.Scatter(
    x=df["date"], y=df["close"],
    name="Close", line=dict(color="#1f77b4", width=1.5)
), row=1, col=1)

ma_map = {"SMA 7": "sma_7", "SMA 21": "sma_21", "SMA 50": "sma_50"}
colors = {"SMA 7": "#ff7f0e", "SMA 21": "#2ca02c", "SMA 50": "#d62728"}
for label in ma_options:
    col = ma_map[label]
    fig.add_trace(go.Scatter(
        x=df["date"], y=df[col],
        name=label, line=dict(color=colors[label], width=1.2, dash="dash")
    ), row=1, col=1)

# MACD
fig.add_trace(go.Scatter(
    x=df["date"], y=df["macd"],
    name="MACD", line=dict(color="#9467bd", width=1.2)
), row=2, col=1)
fig.add_hline(y=0, line_dash="dot", line_color="gray", row=2, col=1)

# Volume bars
fig.add_trace(go.Bar(
    x=df["date"], y=df["volume"],
    name="Volume", marker_color="#aec7e8"
), row=3, col=1)

fig.update_layout(
    height=680,
    showlegend=True,
    margin=dict(l=0, r=0, t=20, b=0),
    hovermode="x unified",
)
fig.update_yaxes(title_text="Price ($)", row=1, col=1)
fig.update_yaxes(title_text="MACD",      row=2, col=1)
fig.update_yaxes(title_text="Volume",    row=3, col=1)

st.plotly_chart(fig, use_container_width=True)

# ── Returns distribution ──────────────────────────────────────────────
st.subheader("Daily return distribution")
hist = go.Figure(go.Histogram(
    x=df["daily_return_pct"].dropna(),
    nbinsx=80,
    marker_color="#1f77b4",
    opacity=0.75,
))
hist.update_layout(height=260, margin=dict(l=0, r=0, t=20, b=0))
st.plotly_chart(hist, use_container_width=True)