from __future__ import annotations

import json
from html import escape
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"

st.set_page_config(
    page_title="NORTHSTAR | Retail Analytics",
    page_icon="✳",
    layout="wide",
)
st.markdown(
    """
    <style>
    .stApp { background: #080d17; color: #eef4ff; }
    [data-testid="stHeader"] { background: rgba(8,13,23,.9); }
    [data-testid="stSidebar"] { background: #101827; }
    .block-container { padding-top: 1.8rem; }
    .hero {
      padding: 1.5rem 1.8rem; border: 1px solid #26354d; border-radius: 18px;
      background: linear-gradient(120deg, #121d30 0%, #102c37 58%, #185044 100%);
      margin-bottom: 1.25rem;
    }
    .eyebrow { color: #6ce2b3; letter-spacing: .17em; font-size: .75rem; font-weight: 700; }
    .hero h1 { margin: .35rem 0; font-size: 2.2rem; }
    .hero p { color: #a9bad0; margin-bottom: 0; }
    [data-testid="stMetric"] {
      background: #101827; border: 1px solid #24344b;
      padding: 1rem; border-radius: 14px;
    }
    [data-testid="stMetricValue"] { color: #6ce2b3; }
    .insight {
      padding: 1rem 1.15rem; background: #101827; border: 1px solid #24344b;
      border-left: 3px solid #6ce2b3; border-radius: 10px; margin: .6rem 0;
    }
    .footnote { color: #94a4b9; font-size: .84rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero">
      <div class="eyebrow">NORTHSTAR · SYNTHETIC RETAIL INTELLIGENCE</div>
      <h1>One source of truth. Every daily decision.</h1>
      <p>BigQuery-ready partitions · tested dbt marts · evidence you can trace.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

required = [
    RESULTS / "monthly_kpis.csv",
    RESULTS / "monthly_segments.csv",
    RESULTS / "metrics.json",
    RESULTS / "evidence.json",
]
missing = [path.name for path in required if not path.is_file()]
if missing:
    st.warning(
        "Run `python -m gcp_analytics.pipeline` first. Missing outputs: "
        + ", ".join(missing)
    )
    st.stop()

monthly = pd.read_csv(required[0], parse_dates=["order_month"])
segments = pd.read_csv(required[1], parse_dates=["order_month"])
metrics = json.loads(required[2].read_text(encoding="utf-8"))
evidence = json.loads(required[3].read_text(encoding="utf-8"))

months = monthly["order_month"].sort_values().tolist()
focus_month = st.sidebar.selectbox(
    "Reporting month",
    months,
    index=len(months) - 1,
    format_func=lambda value: value.strftime("%B %Y"),
)
focus = monthly.loc[monthly["order_month"] == focus_month].iloc[0]
previous = monthly.loc[
    monthly["order_month"] == focus_month - pd.offsets.MonthBegin(1)
]
sales_change = (
    (float(focus["net_sales"]) / float(previous["net_sales"].iloc[0]) - 1) * 100
    if not previous.empty
    else None
)

k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "Net sales",
    f"${focus['net_sales']:,.0f}",
    f"{sales_change:+.1f}% MoM" if sales_change is not None else None,
)
k2.metric("Gross profit", f"${focus['gross_profit']:,.0f}")
k3.metric("Gross margin", f"{focus['gross_margin_rate']:.1%}")
k4.metric("Active customers", f"{focus['active_customers']:,.0f}")

left, right = st.columns([1.4, 1])
with left:
    st.subheader("The trajectory")
    trend = px.area(
        monthly,
        x="order_month",
        y="net_sales",
        markers=True,
        template="plotly_dark",
        color_discrete_sequence=["#6ce2b3"],
    )
    trend.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=8, r=8, t=10, b=8),
        xaxis_title=None,
        yaxis_title="Net sales ($)",
    )
    st.plotly_chart(trend, use_container_width=True)
with right:
    st.subheader("What moved—and where")
    st.markdown(
        f'<div class="insight">{escape(evidence["monthly_sales"]["display"])}</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="insight">{escape(evidence["regional_driver"]["display"])}</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="insight">{escape(evidence["campaign_context"]["display"])}</div>',
        unsafe_allow_html=True,
    )

st.subheader("Regional mix × category")
period = segments.loc[segments["order_month"] == focus_month]
figure = px.treemap(
    period,
    path=["region", "category"],
    values="net_sales",
    color="gross_margin_rate",
    color_continuous_scale=["#f26e5f", "#e7bb55", "#6ce2b3"],
    template="plotly_dark",
    hover_data={"order_count": True, "discount_rate": ":.1%"},
)
figure.update_layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=8, r=8, t=10, b=8),
)
st.plotly_chart(figure, use_container_width=True)

with st.expander("Cost control, measured locally"):
    scan = metrics["partition_pruning_estimate"]
    a, b, c = st.columns(3)
    a.metric("Full-history baseline", f"{scan['unpartitioned_baseline_bytes'] / 1e6:.2f} MB")
    b.metric("One daily partition", f"{scan['selected_daily_partition_bytes'] / 1e3:.1f} KB")
    c.metric(
        "Local file-volume reduction",
        f"{scan['local_object_volume_reduction_pct']:.2f}%",
    )
    st.caption(scan["method"])
    st.warning(scan["warning"])

st.caption(
    f"{metrics['order_count']:,} synthetic orders · "
    f"{metrics['date_range']['start']} — {metrics['date_range']['end']} · "
    f"{metrics['dbt_tests_passed']}/{metrics['dbt_tests_total']} dbt tests passed."
)
st.caption(
    "Synthetic portfolio for engineering demonstration only. Associations are "
    "descriptive, not causal. Local file-size estimates are not BigQuery billing data."
)
