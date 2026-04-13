import streamlit as st
import requests
import pandas as pd
import time

API_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="Logistics Dashboard", layout="wide")
st.title("🚚 Real-Time Logistics Dashboard")

# Sidebar refresh
refresh_rate = st.sidebar.slider("Refresh rate (seconds)", 1, 10, 3)

# Fetch data
def fetch_data():
    try:
        res = requests.get(f"{API_URL}/vehicles")
        return res.json()
    except:
        return []

data = fetch_data()

if data:
    df = pd.DataFrame(data)

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Trucks", len(df))
    col2.metric("In Progress", (df["status"] == "IN_PROGRESS").sum())
    col3.metric("Completed", (df["status"] == "COMPLETED").sum())

    st.divider()

    status_filter = st.selectbox(
        "Filter by Status",
        ["ALL", "IN_PROGRESS", "COMPLETED"],
        key="status_filter"   # ✅ FIX
    )

    if status_filter != "ALL":
        df = df[df["status"] == status_filter]

    st.dataframe(df, use_container_width=True)


    # Highlight delays
    def highlight_delay(row):
        if row["eta_minutes"] >60:
            return ["background-color: red"] * len(row)
        return [""] * len(row)


    st.subheader("📊 Truck Status Table")

    st.dataframe(
        df.style.apply(highlight_delay, axis=1),
        use_container_width=True
    )

    # Top Delays
    st.subheader("🚨 Top Delayed Trucks")

    delayed = df.sort_values(by="eta_minutes", ascending=False).head(5)

    st.dataframe(delayed, use_container_width=True)

else:
    st.warning("No data available...")

# Auto refresh
time.sleep(refresh_rate)
st.rerun()
