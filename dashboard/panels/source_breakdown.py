import streamlit as st
import plotly.graph_objects as go
from collections import Counter
from dashboard.utils.supabase_client import get_source_breakdown

def render_source_breakdown():
    """Renders horizontal bar chart of anomalies by source."""
    st.subheader("Anomalies by Source")

    data = get_source_breakdown()

    if not data:
        st.info("No source data yet.")
        return

    counts = Counter([d["source"] for d in data])
    sources = list(counts.keys())
    values = list(counts.values())

    color_map = {
        "stripe": "#635bff",
        "shopify": "#96bf48",
        "paypal": "#003087"
    }
    colors = [color_map.get(s, "#888888") for s in sources]

    fig = go.Figure(go.Bar(
        x=values,
        y=sources,
        orientation="h",
        marker=dict(color=colors)
    ))

    fig.update_layout(
        xaxis_title="Anomaly Count",
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font=dict(color="white"),
        height=300
    )

    st.plotly_chart(fig, use_container_width=True)