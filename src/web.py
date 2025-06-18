import streamlit as st
import pandas as pd
import altair as alt
import logging
import redis
from datetime import datetime
import asyncio
import json

from websocket_server import mark_random_train_as_inactive, reset_all_trains
from utils.navigate import fetch_ambu_broken_train_positions, calculate_optimal_path
from utils.join_query import record_ambulance_path

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
def load_map_html(filepath="animated_map.html", route_points = None):
    # Load rails GeoJSON
    with open("utils/UtrechtRails.geojson", "r") as f:
        rails_geojson = json.load(f)
    rails_js = json.dumps(rails_geojson)

    # 将 `route_points` 也转换为 JSON 格式
    route_points_js = json.dumps(route_points)

    # 读取 HTML 文件并插入数据
    with open(filepath, "r") as f:
        html = f.read()
        html = html.replace("//__INSERT_RAILS_HERE__", f"const railsData = {rails_js};")
        html = html.replace("//__INSERT_ROUTE_POINTS_HERE__", f"const routePoints = {route_points_js};")

    return html

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

    cols = st.columns(4)

    for i, row in ambulance_data.iterrows():
        usage_percentage = int((row["In Use"] / row["Capacity"]) * 100)
        with cols[i]:
            st.altair_chart(make_pie_chart(usage_percentage, row["Station"], color_options[i]))

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
    routes = await calculate_optimal_path(positions)

    # 确保 routes 不是 None 或者空字典
    if not routes or not isinstance(routes, dict):
        return [], None, None

    # 提取所需数据，并检查 key 是否存在
    ambulance_id = routes.get("ambulance_id", None)
    route_points = [(point["latitude"], point["longitude"]) for point in routes.get("route_points", [])]
    timestamp = routes.get("timestamp", None)
    route_estimated_time = routes.get("route_estimated_time", None)

    await record_ambulance_path(route_points, timestamp, route_estimated_time, ambulance_id)

    # # 显示 JSON 数据
    # st.json(routes)

    # 返回多个值（如果为空，则返回适当的空值）
    return route_points, timestamp, route_estimated_time


# Main dashboard layout setup
async def main():
    with st.sidebar:
        st.title("RCOP Dashboard")

    # Initialize session states
    if 'button_states' not in st.session_state:
        st.session_state['button_states'] = {
            'stop_disabled': False,
            'reset_disabled': True,
            'show_incident': False,
            'show_reset_success': False
        }

    col = st.columns((2.5, 5, 1), gap="small")

    # Left column: ambulance data and charts
    with col[0]:
        ambulance_data = display_ambulance_data()
        
    # Middle column: animated map
    with col[1]:
        route_points, timestamp, route_estimated_time = await fetch_and_display_positions()
        st.components.v1.html(load_map_html(route_points = route_points), height=500, scrolling=False)
        display_availability_charts(ambulance_data)

    # Right column: Incident/Train control
    with col[2]:
        st.markdown("### Train Control")
       
        # Button to stop a random train (simulate incident)
        if st.button(
            "🛑 Simulate an incident",
            disabled=st.session_state['button_states']['stop_disabled'],
            key="stop_train_button"
        ):
            st.session_state['incident_data'] = None
            st.session_state['button_states']['stop_disabled'] = True
            st.session_state['button_states']['reset_disabled'] = False
            st.session_state['button_states']['show_reset_success'] = False
            incident = stop_random_train()
            if incident:
                st.session_state['incident_data'] = incident
                st.session_state['button_states']['show_incident'] = True
            st.rerun()
        st.caption("(This stops a random train)")

        # Button to reset all trains
        if st.button(
            "🔄 Resolve the incident",
            disabled=st.session_state['button_states']['reset_disabled'],
            key="reset_train_button"
        ):
            reset_all_trains(client)
            st.session_state['button_states']['stop_disabled'] = False
            st.session_state['button_states']['reset_disabled'] = True
            st.session_state['button_states']['show_incident'] = False
            st.session_state['button_states']['show_reset_success'] = True
            st.rerun()
        st.caption("(This will reset all trains)")

        st.markdown("---")

        # Display incident notification if active
        if st.session_state['button_states']['show_incident'] and st.session_state.get('incident_data'):
            st.success("An incident was simulated!")
            coords = st.session_state['incident_data']["location"].get("coordinates", [])
            lng, lat = coords[0], coords[1]
            timestamp = int(coords[2]) if len(coords) > 2 else None

            # Create cleaned-up incident dict
            clean_incident = {
                "train_id": st.session_state['incident_data']["train_id"],
                "severity": (st.session_state['incident_data']["severity"]).capitalize(),
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

        # Display reset success notification if active
        if st.session_state['button_states']['show_reset_success']:
            st.success("All trains have been reset to active status!")
        
# Run the dashboard
if __name__ == "__main__":
    asyncio.run(main())