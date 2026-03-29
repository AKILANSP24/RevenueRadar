import streamlit as st
import plotly.graph_objects as go
from dashboard.utils.supabase_client import get_today_stats

def render_anomaly_breakdown():
    """Renders donut chart of today's anomaly severity split."""
    st.subheader("Today's Anomaly Breakdown")

    stats = get_today_stats()
    total = stats.get("total_events", 0)
    anomalies = stats.get("anomaly_count", 0)
    critical = stats.get("critical_count", 0)
    warning = anomalies - critical
    normal = max(0, total - anomalies)

    if total == 0:
        st.info("No events yet today.")
        return

    fig = go.Figure(data=[go.Pie(
        labels=["Normal", "Warning", "Critical"],
        values=[normal, warning, critical],
        hole=0.5,
        marker=dict(colors=["#2ecc71", "#f39c12", "#e74c3c"])
    )])

    fig.update_layout(
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font=dict(color="white"),
        height=300,
        showlegend=True
    )

    st.plotly_chart(fig, use_container_width=True)