import streamlit as st
import pandas as pd
from dashboard.utils.supabase_client import get_recent_events

def render_live_feed():
    """Renders live transaction feed with severity coloring."""
    st.subheader("Live Transaction Feed")

    events = get_recent_events(50)

    if not events:
        st.info("No anomaly events yet. Pipeline is warming up...")
        return

    df = pd.DataFrame(events)

    keep = ["timestamp", "source", "amount",
            "severity", "z_score", "ai_explanation"]
    existing = [c for c in keep if c in df.columns]
    df = df[existing]

    if "amount" in df.columns:
        df["amount"] = df["amount"].apply(lambda x: f"Rs.{x:,.2f}")
    if "z_score" in df.columns:
        df["z_score"] = df["z_score"].apply(lambda x: f"{x:.2f}")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"]).dt.strftime("%H:%M:%S")

    critical = len([e for e in events if e.get("severity") == "critical"])
    warning = len([e for e in events if e.get("severity") == "warning"])

    c1, c2 = st.columns(2)
    c1.metric("Critical", critical, delta=None)
    c2.metric("Warning", warning, delta=None)

    def highlight_row(row):
        if row.get("severity") == "critical":
            return ["background-color: #ffcccc"] * len(row)
        elif row.get("severity") == "warning":
            return ["background-color: #fff3cc"] * len(row)
        return ["background-color: #ccffcc"] * len(row)

    styled = df.style.apply(highlight_row, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True)