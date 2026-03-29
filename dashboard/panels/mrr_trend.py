import streamlit as st
import plotly.graph_objects as go
from dashboard.utils.supabase_client import get_daily_health_scores

def render_mrr_trend():
    """Renders health score trend over last 30 days."""
    st.subheader("Revenue Health Score Trend (30 Days)")

    data = get_daily_health_scores(30)

    if not data:
        st.info("Not enough data yet for trend chart.")
        return

    dates = [d["date"] for d in reversed(data)]
    scores = [d["health_score"] for d in reversed(data)]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=dates, y=scores,
        mode="lines+markers",
        name="Health Score",
        line=dict(color="#2E86DE", width=2),
        marker=dict(size=6)
    ))

    fig.add_hline(
        y=75,
        line_dash="dash",
        line_color="gray",
        annotation_text="Healthy Threshold (75)"
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Score (0-100)",
        yaxis=dict(range=[0, 105]),
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font=dict(color="white"),
        height=350
    )

    st.plotly_chart(fig, use_container_width=True)