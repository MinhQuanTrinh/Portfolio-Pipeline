import streamlit as st
import duckdb
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

st.set_page_config(page_title="ASX Stock Analytics", layout="wide")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/tmp/stock.duckdb")

TICKER_INFO = {
    "LDX.AX": {
        "name": "Lumos Diagnostics",
        "description": "Point-of-care diagnostics company focused on rapid testing solutions.",
        "links": [
            {"label": "ASX Announcements", "url": "https://www.asx.com.au/markets/company/LDX"},
            {"label": "Reuters Profile", "url": "https://www.reuters.com/markets/companies/LDX.AX"},
            {"label": "Simply Wall St", "url": "https://simplywall.st/stocks/au/pharma-biotech/asx-ldx/lumos-diagnostics-shares"},
        ],
    },
    "4DX.AX": {
        "name": "4DMedical",
        "description": "Medical imaging technology company specialising in lung function assessment.",
        "links": [
            {"label": "ASX Announcements", "url": "https://www.asx.com.au/markets/company/4DX"},
            {"label": "Reuters Profile", "url": "https://www.reuters.com/markets/companies/4DX.AX"},
            {"label": "Simply Wall St", "url": "https://simplywall.st/stocks/au/pharma-biotech/asx-4dx/4dmedical-shares"},
        ],
    },
    "CU6.AX": {
        "name": "Clarity Pharmaceuticals",
        "description": "Clinical-stage radiopharmaceutical company developing copper-based therapies.",
        "links": [
            {"label": "ASX Announcements", "url": "https://www.asx.com.au/markets/company/CU6"},
            {"label": "Reuters Profile", "url": "https://www.reuters.com/markets/companies/CU6.AX"},
            {"label": "Simply Wall St", "url": "https://simplywall.st/stocks/au/pharma-biotech/asx-cu6/clarity-pharmaceuticals-shares"},
        ],
    },
    "PME.AX": {
        "name": "Pro Medicus",
        "description": "Healthcare IT company providing radiology software and AI-powered imaging solutions.",
        "links": [
            {"label": "ASX Announcements", "url": "https://www.asx.com.au/markets/company/PME"},
            {"label": "Reuters Profile", "url": "https://www.reuters.com/markets/companies/PME.AX"},
            {"label": "Simply Wall St", "url": "https://simplywall.st/stocks/au/tech/asx-pme/pro-medicus-shares"},
        ],
    },
}


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_bollinger(series: pd.Series, period: int = 20, std: int = 2):
    mid   = series.rolling(period).mean()
    sigma = series.rolling(period).std()
    return mid, mid + std * sigma, mid - std * sigma


def compute_macd_signal(series: pd.Series, fast=12, slow=26, signal=9):
    ema_fast   = series.ewm(span=fast, adjust=False).mean()
    ema_slow   = series.ewm(span=slow, adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram  = macd_line - signal_line
    return macd_line, signal_line, histogram


@st.cache_data(ttl=3600)
def load_data(ticker: str) -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df  = con.execute(f"""
        SELECT * FROM main.mart_dashboard
        WHERE ticker = '{ticker}'
        ORDER BY date
    """).df()
    con.close()
    return df


# ── Sidebar ───────────────────────────────────────────────────────────
st.sidebar.title("Controls")
ticker = st.sidebar.selectbox(
    "Ticker",
    list(TICKER_INFO.keys()),
    format_func=lambda t: f"{t.split('.')[0]} — {TICKER_INFO[t]['name']}"
)

ma_options = st.sidebar.multiselect(
    "Moving averages",
    ["SMA 7", "SMA 21", "SMA 50"],
    default=["SMA 21", "SMA 50"],
)

show_bb      = st.sidebar.checkbox("Bollinger Bands", value=True)
show_rsi     = st.sidebar.checkbox("RSI (14)", value=True)
show_atr     = st.sidebar.checkbox("ATR (14)", value=False)

df = load_data(ticker)

if df.empty:
    st.warning("No data found. Run the pipeline and dbt first.")
    st.stop()

start_date, end_date = st.sidebar.select_slider(
    "Date range",
    options=sorted(df["date"].astype(str).unique()),
    value=(df["date"].astype(str).min(), df["date"].astype(str).max()),
)
df = df[(df["date"].astype(str) >= start_date) & (df["date"].astype(str) <= end_date)].copy()

# ── Compute indicators ────────────────────────────────────────────────
df["rsi"]                              = compute_rsi(df["close"])
df["bb_mid"], df["bb_up"], df["bb_lo"] = compute_bollinger(df["close"])
df["macd_line"], df["signal_line"], df["macd_hist"] = compute_macd_signal(df["close"])

# ── Header ────────────────────────────────────────────────────────────
info   = TICKER_INFO[ticker]
latest = df.iloc[-1]
prev   = df.iloc[-2]
delta  = latest["close"] - prev["close"]
delta_pct = (delta / prev["close"]) * 100

st.title(f"📈 {ticker} — {info['name']}")
st.caption(info["description"])

# Article links
link_cols = st.columns(len(info["links"]))
for col, link in zip(link_cols, info["links"]):
    col.link_button(link["label"], link["url"])

st.divider()

# ── KPI cards ─────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Close",       f"${latest['close']:.2f}",         f"{delta:+.2f} ({delta_pct:+.1f}%)")
col2.metric("Volume",      f"{latest['volume']/1e6:.2f}M")
col3.metric("Volatility",  f"{latest['volatility_pct']:.1f}%")
col4.metric("ATR (14)",    f"${latest['atr_14']:.2f}")
col5.metric("Signal",      latest["signal"].capitalize())

st.divider()

# ── Build subplot layout ──────────────────────────────────────────────
row_count   = 2 + int(show_rsi) + int(show_atr)
row_heights = [0.50, 0.25] + ([0.15] * (row_count - 2))
subplot_titles = ["Price", "MACD"]
if show_rsi: subplot_titles.append("RSI (14)")
if show_atr: subplot_titles.append("ATR (14)")

fig = make_subplots(
    rows=row_count, cols=1,
    shared_xaxes=True,
    row_heights=row_heights,
    vertical_spacing=0.04,
    subplot_titles=subplot_titles,
)

# ── Row 1: Price + overlays ───────────────────────────────────────────
fig.add_trace(go.Scatter(
    x=df["date"], y=df["close"],
    name="Close", line=dict(color="#1f77b4", width=1.5)
), row=1, col=1)

ma_map    = {"SMA 7": "sma_7", "SMA 21": "sma_21", "SMA 50": "sma_50"}
ma_colors = {"SMA 7": "#ff7f0e", "SMA 21": "#2ca02c", "SMA 50": "#d62728"}
for label in ma_options:
    fig.add_trace(go.Scatter(
        x=df["date"], y=df[ma_map[label]],
        name=label, line=dict(color=ma_colors[label], width=1.2, dash="dash")
    ), row=1, col=1)

if show_bb:
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["bb_up"],
        name="BB Upper", line=dict(color="rgba(128,128,128,0.4)", width=1),
        showlegend=False,
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["bb_lo"],
        name="BB Lower", line=dict(color="rgba(128,128,128,0.4)", width=1),
        fill="tonexty", fillcolor="rgba(128,128,128,0.08)",
        showlegend=False,
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["bb_mid"],
        name="BB Mid", line=dict(color="rgba(128,128,128,0.5)", width=1, dash="dot"),
        showlegend=False,
    ), row=1, col=1)

# ── Row 2: MACD ───────────────────────────────────────────────────────
colors_hist = ["#2ca02c" if v >= 0 else "#d62728" for v in df["macd_hist"]]

fig.add_trace(go.Bar(
    x=df["date"], y=df["macd_hist"],
    name="MACD Hist", marker_color=colors_hist, opacity=0.6,
), row=2, col=1)
fig.add_trace(go.Scatter(
    x=df["date"], y=df["macd_line"],
    name="MACD", line=dict(color="#1f77b4", width=1.2)
), row=2, col=1)
fig.add_trace(go.Scatter(
    x=df["date"], y=df["signal_line"],
    name="Signal", line=dict(color="#ff7f0e", width=1.2)
), row=2, col=1)
fig.add_hline(y=0, line_dash="dot", line_color="gray", row=2, col=1)

# ── Row 3+: Optional indicators ───────────────────────────────────────
current_row = 3

if show_rsi:
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["rsi"],
        name="RSI", line=dict(color="#9467bd", width=1.2)
    ), row=current_row, col=1)
    fig.add_hline(y=70, line_dash="dash", line_color="red",   opacity=0.5, row=current_row, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", opacity=0.5, row=current_row, col=1)
    fig.update_yaxes(range=[0, 100], row=current_row, col=1)
    current_row += 1

if show_atr:
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["atr_14"],
        name="ATR 14", line=dict(color="#8c564b", width=1.2), fill="tozeroy",
        fillcolor="rgba(140,86,75,0.08)",
    ), row=current_row, col=1)

fig.update_layout(
    height=200 + (250 * row_count),
    showlegend=True,
    margin=dict(l=0, r=0, t=30, b=0),
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
)
fig.update_yaxes(title_text="Price (AUD)", row=1, col=1)
fig.update_yaxes(title_text="MACD",        row=2, col=1)

st.plotly_chart(fig, use_container_width=True)

# ── Returns distribution ──────────────────────────────────────────────
st.subheader("Daily return distribution")
hist = go.Figure(go.Histogram(
    x=df["daily_return_pct"].dropna(),
    nbinsx=80,
    marker_color="#1f77b4",
    opacity=0.75,
))
hist.add_vline(x=0, line_dash="dash", line_color="gray")
hist.update_layout(
    height=260,
    margin=dict(l=0, r=0, t=20, b=0),
    xaxis_title="Daily return (%)",
    yaxis_title="Count",
)
st.plotly_chart(hist, use_container_width=True)

# ── Summary stats ─────────────────────────────────────────────────────
st.subheader("Summary statistics")
stats = df[["close", "volume", "daily_return_pct", "volatility_pct", "atr_14"]].describe().T
stats.columns = ["Count", "Mean", "Std", "Min", "25%", "50%", "75%", "Max"]
st.dataframe(stats.style.format("{:.2f}"), use_container_width=True)