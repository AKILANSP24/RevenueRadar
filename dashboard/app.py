import streamlit as st
import time
import sys
import os

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")))

from dashboard.panels.health_score import render_health_score
from dashboard.panels.live_feed import render_live_feed
from dashboard.panels.mrr_trend import render_mrr_trend
from dashboard.panels.anomaly_breakdown import render_anomaly_breakdown
from dashboard.panels.source_breakdown import render_source_breakdown
from dashboard.panels.heatmap import render_heatmap

st.set_page_config(
    page_title="RevenueRadar",
    page_icon="lightning",
    layout="wide"
)

st.markdown(
    """
    <h1 style='text-align:center; color:#2E86DE;'>
    RevenueRadar</h1>
    <p style='text-align:center; color:gray;'>
    Real-Time Revenue Anomaly Intelligence Platform</p>
    <hr>
    """,
    unsafe_allow_html=True
)

with st.sidebar:
    st.title("RevenueRadar")
    st.markdown("Cloud-native revenue anomaly detection.")
    st.markdown("---")
    auto_refresh = st.toggle("Auto Refresh (30s)", value=True)
    st.markdown("---")
    st.markdown("**Sources:** Stripe | Shopify | PayPal")
    st.markdown("**Engine:** Temporal Baseline Z-Score")
    st.markdown("**Storage:** Supabase + Parquet")

render_health_score()
st.markdown("---")

col1, col2 = st.columns(2)
with col1:
    render_anomaly_breakdown()
with col2:
    render_source_breakdown()

st.markdown("---")
render_mrr_trend()
st.markdown("---")

col3, col4 = st.columns(2)
with col3:
    render_live_feed()
with col4:
    render_heatmap()

if auto_refresh:
    time.sleep(30)
    st.rerun()