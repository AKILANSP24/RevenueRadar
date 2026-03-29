import streamlit as st
from dashboard.utils.supabase_client import get_today_stats

def render_health_score():
    """Renders the Revenue Health Score panel."""
    st.subheader("Revenue Health Score")
    stats = get_today_stats()

    score = stats.get("health_score", 100.0)
    total = stats.get("total_events", 0)
    anomalies = stats.get("anomaly_count", 0)
    critical = stats.get("critical_count", 0)

    if score >= 75:
        color = "green"
        status = "Healthy"
    elif score >= 50:
        color = "orange"
        status = "Warning"
    else:
        color = "red"
        status = "Critical"

    st.markdown(
        f"""
        <div style='text-align:center; padding:20px;
        border-radius:10px; background:#1e1e2e;'>
        <h1 style='color:{color}; font-size:72px;
        margin:0;'>{score:.1f}</h1>
        <p style='color:{color}; font-size:20px;
        margin:0;'>{status}</p>
        <p style='color:gray; font-size:14px;'>
        out of 100</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Events", total)
    c2.metric("Anomalies", anomalies)
    c3.metric("Critical", critical)