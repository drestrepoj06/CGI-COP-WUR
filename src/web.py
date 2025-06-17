import streamlit as st
import pandas as pd
import altair as alt
import logging
import redis
from datetime import datetime
import asyncio

from websocket_server import mark_random_train_as_inactive
from utils.navigate import fetch_ambu_broken_train_positions

# Streamlit app setup
st.set_page_config(
    page_title="RCOP Dashboard",
    page_icon="🚅",
    layout="wide",
    initial_sidebar_state="collapsed",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Donut chart generator
def make_pie_chart(usage_percentage, station_name, color_scheme):
    colors = {
        'blue': ['#29b5e8', '#155F7A'],
        'green': ['#27AE60', '#12783D'],
        'red': ['#E74C3C', '#781F16'],
        'yellow': ["#F9ED00", "#A79D09"]
    }

    source = pd.DataFrame({
        "Status": ['Available', 'In Use'],
        "Value": [100 - usage_percentage, usage_percentage],
    })

    return alt.Chart(source).mark_arc().encode(
        theta="Value:Q",
        color=alt.Color("Status:N", scale=alt.Scale(range=colors[color_scheme]),
                        legend=alt.Legend(title="Ambulance Status")),
        tooltip=["Status:N", "Value:Q"]
    ).properties(width=130, height=130, title=station_name)

# Load animated map HTML
def load_map_html(filepath="animated_map.html"):
    with open(filepath, "r") as f:
        return f.read()

# Display ambulance data table
def display_ambulance_data():
    st.subheader("Ambulance station availability")

    # Sample data representing station capacities and availability
    ambulance_data = pd.DataFrame({
        "Station": ["Maarssen", "Vader Rijndreef", "Diakonessenhuis", "On the move"],
        "Ambulances": [3, 5, 2, 5],
        "Capacity": [5, 6, 4, 8]
    })

    ambulance_data["In Use"] = ambulance_data["Capacity"] - ambulance_data["Ambulances"]

    st.dataframe(
        ambulance_data,
        column_order=("Station", "Ambulances"),
        hide_index=True,
        column_config={
            "Station": st.column_config.TextColumn("Station", width="small"),
            "Ambulances": st.column_config.ProgressColumn("Available", format="%d", min_value=0, max_value=max(ambulance_data["Capacity"]), width="small")
        }
    )

    return ambulance_data

# Display ambulance availability charts
def display_availability_charts(ambulance_data):
    st.subheader("Ambulance availability")

    color_options = ['red', 'green', 'blue', 'yellow']

    for i, row in ambulance_data.iterrows():
        usage_percentage = int((row["In Use"] / row["Capacity"]) * 100)
        st.altair_chart(make_pie_chart(usage_percentage, row["Station"], color_options[i]), use_container_width=True)

# Redis client
client = redis.Redis(host="tile38", port=9851, decode_responses=True)

# Synchronous function to mark a random train as inactive
def stop_random_train():
    try:
        result = mark_random_train_as_inactive(client)
        logging.info("✅ Marked one random train as inactive!")
        return result
    except Exception as e:
        logging.error(f"[ERROR] mark_random_train_as_inactive() failed: {e}")
        return False

async def fetch_and_display_positions():
    """

    Fetch ambulance and broken train positions and display them in the dashboard.
    """
    positions = await fetch_ambu_broken_train_positions()
    # routes = calculate_optimal_path(positions) 

    st.json(positions)  # Display the fetched data in JSON format

# Main dashboard layout setup
async def main():
    with st.sidebar:
        st.title("RCOP Dashboard")

    col = st.columns((2.5, 5, 1), gap="small")

    # Left column: ambulance data and charts
    with col[0]:
        ambulance_data = display_ambulance_data()
        display_availability_charts(ambulance_data)

    # Middle column: animated map
    with col[1]:
        st.components.v1.html(load_map_html(), height=500, scrolling=False)
        await fetch_and_display_positions()

    # Right column: Incident/Train control
    with col[2]:
        st.markdown("### Train Control")
        # Button to stop a random train (main requested feature)
        if st.button("🛑 Stop a Random Train"):
            st.session_state['incident_data'] = None
            incident = stop_random_train()
            if incident:
                st.session_state['incident_data'] = incident
                st.session_state['train_stop_result'] = "success"
            else:
                st.session_state['train_stop_result'] = "fail"
            st.rerun()
        train_stop_result = st.session_state.get('train_stop_result', None)
        incident_data = st.session_state.get('incident_data', None)

        if train_stop_result == "success" and incident_data:
            st.success("An incident was simulated!")

            coords = incident_data["location"].get("coordinates", [])
            lng, lat = coords[0], coords[1]
            timestamp = int(coords[2]) if len(coords) > 2 else None  # if 3D point with timestamp

            # Create cleaned-up incident dict
            clean_incident = {
                "train_id": incident_data["train_id"],
                "severity": incident_data["severity"],
                "lat": lat,
                "lng": lng,
                "timestamp": timestamp
            }

            # Display as JSON or table
            ts = clean_incident["timestamp"]
            readable_time = datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')

            st.markdown(f"""
            ### 🚨 Incident Summary  
            **Train ID**: `{clean_incident['train_id']}`  
            **Severity**: `{clean_incident['severity']}`  
            **Location**: `{clean_incident['lat']}, {clean_incident['lng']}`  
            **Timestamp**: `{readable_time} UTC`
            """)
            
        elif train_stop_result == "fail":
            st.error("Failed to mark a random train as inactive.")

# Run the dashboard
if __name__ == "__main__":
    asyncio.run(main())
