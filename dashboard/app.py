import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

st.set_page_config(page_title="Portfolio Dashboard", layout="wide")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/tmp/stock.duckdb")

#Gather articles for each ticker
TICKER_INFO = {
    "LDX.AX": {
        "name": "Lumos Diagnostics", "LDX",
        "link": [
            {"label": "ASX Announcements", "url": "https://www.asx.com.au/markets/company/LDX" },
            {"label": "Hot Copper Discussions", "url": "https://hotcopper.com.au/asx/ldx/"},
        ] ,
    },
    "4DX.AX": {
        "name": "4DMedical",
        "links": [
            {"label": "ASX Announcements", "url": "https://www.asx.com.au/markets/company/4DX"},
            {"label": "Reuters Profile", "url": "https://www.reuters.com/markets/companies/4DX.AX"},
            {"label": "Simply Wall St", "url": "https://simplywall.st/stocks/au/pharma-biotech/asx-4dx/4dmedical-shares"},
        ],
    },
    "CU6.AX": {
        "name": "Clarity Pharmaceuticals",
        "links": [
            {"label": "ASX Announcements", "url": "https://www.asx.com.au/markets/company/CU6"},
            {"label": "Reuters Profile", "url": "https://www.reuters.com/markets/companies/CU6.AX"},
            {"label": "Simply Wall St", "url": "https://simplywall.st/stocks/au/pharma-biotech/asx-cu6/clarity-pharmaceuticals-shares"},
        ],
    },
    "PME.AX": {
        "name": "Pro Medicus",
        "links": [
            {"label": "ASX Announcements", "url": "https://www.asx.com.au/markets/company/PME"},
            {"label": "Reuters Profile", "url": "https://www.reuters.com/markets/companies/PME.AX"},
            {"label": "Simply Wall St", "url": "https://simplywall.st/stocks/au/tech/asx-pme/pro-medicus-shares"},
        ],
    },
},

# MACD indicator
def compute_macd_signal(series: pd.Series, fast=12, slow=26, signal=9):
    ema_fast   = series.ewm(span=fast, adjust=False).mean()
    ema_slow   = series.ewm(span=slow, adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram  = macd_line - signal_line
    return macd_line, signal_line, histogram

# RSI indicator
def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

@st.cache_data(ttl=3600)
def load_data(ticker: str) -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute(f"""
        SELECT * FROM main.mart_dashboard
        WHERE ticker = '{ticker}'
        ORDER BY date
    """).df()
    con.close()
    return df


# ── Sidebar ──────────────────────────────────────────────────────────
st.sidebar.title("Controls")
ticker = st.sidebar.selectbox(
    "Ticker",
    ["LDX.AX", "4DX.AX", "CU6.AX", "PME.AX"],
    format_func=lambda t: {
        "LDX.AX": "LDX — Lumos Diagnostics",
        "4DX.AX": "4DX — 4DMedical",
        "CU6.AX": "CU6 — Clarity Pharmaceuticals",
        "PME.AX": "PME — Pro Medicus",
    }.get(t, t)
)

ma_options = st.sidebar.multiselect(
    "Moving averages",
    ["SMA 7", "SMA 21", "SMA 50"],
    default=["SMA 21", "SMA 50"],
)

df = load_data(ticker)

if df.empty:
    st.warning("No data found. Run the pipeline and dbt first.")
    st.stop()

start_date, end_date = st.sidebar.select_slider(
    "Date range",
    options=sorted(df["date"].astype(str).unique()),
    value=(df["date"].astype(str).min(), df["date"].astype(str).max()),
)
df = df[(df["date"].astype(str) >= start_date) & (df["date"].astype(str) <= end_date)]

# ── KPI cards ────────────────────────────────────────────────────────
st.title(f"📈 {ticker} — ASX Stock Analytics")

latest = df.iloc[-1]
prev   = df.iloc[-2]
delta  = latest["close"] - prev["close"]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Close",      f"${latest['close']:.2f}",        f"{delta:+.2f}")
col2.metric("Volume",     f"{latest['volume']/1e6:.2f}M")
col3.metric("Volatility", f"{latest['volatility_pct']:.1f}%")
col4.metric("Signal",     latest["signal"].capitalize())

# ── Price + Moving Average chart ──────────────────────────────────────
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
    fig.add_trace(go.Scatter(
        x=df["date"], y=df[ma_map[label]],
        name=label, line=dict(color=colors[label], width=1.2, dash="dash")
    ), row=1, col=1)

fig.add_trace(go.Scatter(
    x=df["date"], y=df["macd"],
    name="MACD", line=dict(color="#9467bd", width=1.2)
), row=2, col=1)
fig.add_hline(y=0, line_dash="dot", line_color="gray", row=2, col=1)

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