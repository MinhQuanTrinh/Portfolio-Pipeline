import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotply.subplots import make_subplots
import duckdb, os

st.set_page_config(page_title="Reverse DCF Model", layout="wide")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/tmp/stock.duckdb")

#Company Fundamentals
#Sources: latest annual reports/Quarterly reports/ASX Fillings.
FUNDAMENTAL_DATA = {
    "LDX.AX": {
        "name": "Lumos Diagnostics",
        "description": "Point-of-care diagnostics — rapid testing solutions",
        "revenue":          8.5,
        "fcf":            -12.0,
        "shares_out":     310.0,
        "net_cash":        10.0,
        "tax_rate":        0.30,
        "capex_pct":       0.06,
        "links": [
            {"label": "FY2024 Annual Report", "url": "https://www.asx.com.au/markets/company/LDX"},
            {"label": "LDX Investor Centre",  "url": "https://lumosdiagnostics.com/investors/"},
        ],
    },
    "4DX.AX": {
        "name": "4DMedical",
        "description": "Medical imaging — lung function assessment technology",
        "revenue":         10.2,
        "fcf":            -18.0,   # Cash burn stage
        "shares_out":     175.0,
        "net_cash":        28.0,
        "tax_rate":        0.30,
        "capex_pct":       0.08,
        "links": [
            {"label": "FY2024 Annual Report", "url": "https://www.asx.com.au/markets/company/4DX"},
            {"label": "4DX Investor Centre",  "url": "https://4dmedical.com/investors/"},
        ],
    }
}

def get_latest_price(ticker: str) -> float:
    try:
        conn =  duckdb.connect(DUCKDB_PATH, read_only=True)
        row = conn.execute(f"""
            SELECT close FROM main.mart_dashboard
            WHERE ticker = '{ticker}'
            ORDER BY date DESC LIMIT 1
        """).fetchone()
        conn.close()
        return float(row[0]) if row else 0.0
    except Exception as e:
        st.error(f"Error getting latest price: {e}")
        return 0.0

def reverse_dcf(
    current_price: float,
    revenue: float,
    fcf: float,
    shares_out: float,
    net_cash: float,
    wacc: float,
    terminal_growth: float,
    projection_years: int,
    fcf_margin_target: float,
    tax_rate: float,
) -> dict:
    """ Solve for the implied revenue growth rate that makes NPV == Current market cap.
        Uses Binary Search over growth rates to find the closest match."""
    market_cap = current_price * shares_out
    enterprise_val = market_cap - net_cash

    def npv_at_growth(g: float) -> float:
        rev = revenue
        total_pv = 0.0
        for t in range(1, projection_years + 1):
            rev *= (1 + g)
            cf   = rev * fcf_margin_target * (1 - tax_rate)
            pv   = cf / ((1 + wacc) ** t)
            total_pv += pv
        #Terminal Value
        terminal_cf = rev * fcf_margin_target * (1 - tax_rate) * (1 + terminal_growth)
        terminal_val = terminal_cf / (wacc - terminal_growth)
        total_pv += terminal_val / ((1 + wacc) ** projection_years)
        return total_pv
    
    #Binary Sear ch for Growth Rate
    lo, hi = -0.30, 2.00
    for _ in range(100):
        mid = (lo + hi) / 2
        if npv_at_growth(mid) > enterprise_val:
            hi = mid
        else:
            lo = mid
    implied_growth = (lo + hi) / 2

    #scenario table
    growth_rates = np.linspace(max(implied_growth - 0.30, -0.20), implied_growth + 0.40, 9)
    rows = []
    for g in growth_rates:
        pv = npv_at_growth(g)
        imp_price = (pv + net_cash) / shares_out
        upside = (imp_price / current_price -1) * 100
        rows.append({
            "Growth Rate": f"{g*100:.1f}%",
            "Implied Price": f"${imp_price:.2f}",
            "vs Current": f"{upside:+.1f}%",
            "Undervalued": upside > 0,
            "_growth": g,
            "_price": imp_price,
            "_upside": upside,
        })
    return {
        "implied_growth": implied_growth,
        "enterprise_value": enterprise_val,
        "market_cap": market_cap,
        "scenarios": rows,
        "npv_fn": npv_at_growth,
    }

# ── Page header ───────────────────────────────────────────────────────
st.title("🔬 Reverse DCF Model")
st.markdown("""
A standard DCF projects cash flows to find intrinsic value.
A **Reverse DCF** takes the current market price and solves for the implied growth rate —
revealing exactly what growth the market is betting on.
""")

with st.expander("📚 How to use this model", expanded=False):
    st.markdown("""
    **Step 1** — Select a ticker and review the pre-filled fundamentals from the latest annual report.

    **Step 2** — Adjust the model assumptions (WACC, terminal growth, projection period) to reflect your view.

    **Step 3** — Read the **implied growth rate** — this is what the market is pricing in at the current share price.

    **Step 4** — Judge whether that growth rate is realistic:
    - If implied growth seems **too optimistic** → stock may be overvalued
    - If implied growth seems **achievable or conservative** → stock may be fairly valued or cheap

    **Step 5** — Use the scenario table to find what price is justified at different growth assumptions.

    > ⚠️ Fundamentals are manually maintained from the latest ASX filings. Update them each reporting season.
    """)

st.divider()

# ── Ticker selection ──────────────────────────────────────────────────
ticker = st.selectbox(
    "Select ticker",
    list(FUNDAMENTALS.keys()),
    format_func=lambda t: f"{t} — {FUNDAMENTALS[t]['name']}"
)

fund   = FUNDAMENTALS[ticker]
price  = get_latest_price(ticker)

# Article links
st.markdown(f"**{fund['description']}**")
link_cols = st.columns(len(fund["links"]))
for col, link in zip(link_cols, fund["links"]):
    col.link_button(link["label"], link["url"])

st.divider()

# ── Two column layout: inputs left, results right ─────────────────────
left, right = st.columns([1, 2], gap="large")

with left:
    st.subheader("Model inputs")

    st.markdown("**Market data**")
    current_price = st.number_input(
        "Current price (AUD)",
        value=float(round(price, 2)) if price > 0 else 1.0,
        step=0.01, min_value=0.01,
        help="Auto-filled from your pipeline. Edit if stale."
    )
    shares_out = st.number_input(
        "Shares outstanding (M)",
        value=float(fund["shares_out"]), step=1.0, min_value=0.1
    )
    net_cash = st.number_input(
        "Net cash / (debt) (AUD M)",
        value=float(fund["net_cash"]), step=1.0,
        help="Positive = net cash, negative = net debt"
    )

    st.markdown("**Fundamentals**")
    revenue = st.number_input(
        "Annual revenue (AUD M)",
        value=float(fund["revenue"]), step=1.0, min_value=0.1
    )
    fcf_margin = st.slider(
        "Target FCF margin (%)",
        min_value=-50, max_value=60,
        value=int(fund["fcf"] / fund["revenue"] * 100) if fund["revenue"] > 0 else 10,
        help="Long-term normalised free cash flow margin"
    ) / 100

    st.markdown("**Discount rate assumptions**")
    wacc = st.slider(
        "WACC (%)", min_value=6, max_value=20, value=10,
        help="Weighted average cost of capital"
    ) / 100
    terminal_growth = st.slider(
        "Terminal growth rate (%)", min_value=1, max_value=5, value=3,
        help="Long-run growth rate after projection period"
    ) / 100
    years = st.slider(
        "Projection period (years)", min_value=5, max_value=15, value=10
    )

with right:
    result = reverse_dcf(
        current_price   = current_price,
        revenue         = revenue,
        fcf             = fund["fcf"],
        shares_out      = shares_out,
        net_cash        = net_cash,
        wacc            = wacc,
        terminal_growth = terminal_growth,
        projection_years = years,
        fcf_margin_target = fcf_margin,
        tax_rate        = fund["tax_rate"],
    )

    implied_g = result["implied_growth"]

    st.subheader("Model output")

    # KPI cards
    k1, k2, k3 = st.columns(3)
    k1.metric("Market Cap",       f"${result['market_cap']:,.0f}M")
    k2.metric("Enterprise Value", f"${result['enterprise_value']:,.0f}M")
    k3.metric(
        "Implied Growth Rate",
        f"{implied_g*100:.1f}% p.a.",
        help=f"The market is pricing in {implied_g*100:.1f}% annual revenue growth over {years} years"
    )

    # Verdict
    if implied_g > 0.50:
        verdict_color = "🔴"
        verdict = f"The market is pricing in **{implied_g*100:.1f}% annual growth** — extremely aggressive. Leaves little room for error."
    elif implied_g > 0.25:
        verdict_color = "🟡"
        verdict = f"The market is pricing in **{implied_g*100:.1f}% annual growth** — ambitious but potentially achievable for a high-quality compounder."
    elif implied_g > 0.10:
        verdict_color = "🟢"
        verdict = f"The market is pricing in **{implied_g*100:.1f}% annual growth** — moderate expectations. Reasonable for a growing business."
    elif implied_g > 0:
        verdict_color = "🟢"
        verdict = f"The market is pricing in **{implied_g*100:.1f}% annual growth** — conservative. Could be undervalued if the business executes."
    else:
        verdict_color = "⚪"
        verdict = f"The implied growth is **{implied_g*100:.1f}%** — the market expects decline or the company is pre-revenue."

    st.info(f"{verdict_color} {verdict}")

    st.divider()

    # ── Scenario chart ────────────────────────────────────────────────
    st.subheader("Scenario analysis — price at different growth rates")

    scenarios = result["scenarios"]
    growth_vals = [r["_growth"] * 100 for r in scenarios]
    price_vals  = [r["_price"] for r in scenarios]
    bar_colors  = ["#2ca02c" if r["_upside"] > 0 else "#d62728" for r in scenarios]

    fig = make_subplots(rows=1, cols=1)
    fig.add_trace(go.Bar(
        x=[f"{g:.1f}%" for g in growth_vals],
        y=price_vals,
        marker_color=bar_colors,
        text=[f"${p:.2f}" for p in price_vals],
        textposition="outside",
        name="Implied price",
    ))
    fig.add_hline(
        y=current_price,
        line_dash="dash", line_color="#1f77b4", line_width=2,
        annotation_text=f"Current: ${current_price:.2f}",
        annotation_position="top right",
    )
    fig.update_layout(
        height=380,
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis_title="Annual revenue growth rate",
        yaxis_title="Implied share price (AUD)",
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Scenario table ────────────────────────────────────────────────
    st.subheader("Scenario table")
    df_scenarios = pd.DataFrame([{
        "Growth Rate":    r["Growth Rate"],
        "Implied Price":  r["Implied Price"],
        "vs Current":     r["vs Current"],
    } for r in scenarios])

    def highlight_row(row):
        upside = float(row["vs Current"].replace("%", "").replace("+", ""))
        color  = "background-color: rgba(44,160,44,0.15)" if upside > 0 else "background-color: rgba(214,39,40,0.15)"
        return [color] * len(row)

    st.dataframe(
        df_scenarios.style.apply(highlight_row, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    # ── Sensitivity: price vs WACC and growth ─────────────────────────
    st.subheader("Sensitivity — implied price by growth & WACC")

    growth_range = np.linspace(0.0, implied_g * 1.8, 8)
    wacc_range   = [0.08, 0.09, 0.10, 0.11, 0.12, 0.13]

    sensitivity_data = []
    for g in growth_range:
        row_data = {"Growth": f"{g*100:.0f}%"}
        for w in wacc_range:
            npv = sum(
                revenue * (1 + g) ** t * fcf_margin * (1 - fund["tax_rate"]) / (1 + w) ** t
                for t in range(1, years + 1)
            )
            tv  = revenue * (1 + g) ** years * fcf_margin * (1 - fund["tax_rate"]) * (1 + terminal_growth)
            tv /= (w - terminal_growth)
            npv += tv / (1 + w) ** years
            imp = (npv + net_cash) / shares_out
            row_data[f"WACC {w*100:.0f}%"] = f"${imp:.2f}"
        sensitivity_data.append(row_data)

    st.dataframe(
        pd.DataFrame(sensitivity_data).set_index("Growth"),
        use_container_width=True,
    )