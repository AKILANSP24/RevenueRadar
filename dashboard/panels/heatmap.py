import streamlit as st
import plotly.graph_objects as go
from datetime import datetime
from collections import defaultdict
from dashboard.utils.supabase_client import get_anomaly_counts_by_hour_day

def render_heatmap():
    """Renders 7x24 anomaly frequency heatmap."""
    st.subheader("Anomaly Frequency Heatmap (Hour x Day)")

    data = get_anomaly_counts_by_hour_day()

    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    matrix = defaultdict(int)

    for record in data:
        try:
            ts = datetime.fromisoformat(
                record["timestamp"].replace("Z", "+00:00"))
            hour = ts.hour
            day = ts.weekday()
            matrix[(day, hour)] += 1
        except Exception:
            continue

    z = []
    for d in range(7):
        row = [matrix[(d, h)] for h in range(24)]
        z.append(row)

    fig = go.Figure(data=go.Heatmap(
        z=z,
        x=list(range(24)),
        y=days,
        colorscale=[
            [0, "#2ecc71"],
            [0.5, "#f39c12"],
            [1, "#e74c3c"]
        ],
        showscale=True
    ))

    fig.update_layout(
        xaxis_title="Hour of Day",
        yaxis_title="Day of Week",
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font=dict(color="white"),
        height=320
    )

    st.plotly_chart(fig, use_container_width=True)