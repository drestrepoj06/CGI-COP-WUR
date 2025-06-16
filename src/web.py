import streamlit as st
import pandas as pd
import altair as alt
import logging

from websocket_server import mark_random_train_as_inactive

import redis
import asyncio

# Streamlit app setup
st.set_page_config(
    page_title="RCOP Dashboard",
    page_icon="🚅",
    layout="wide",
    initial_sidebar_state="collapsed",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Function to create a pie (donut) chart
def make_pie_chart(usage_percentage, station_name, color_scheme):
    """
    Generates a donut chart displaying ambulance availability status.

    Parameters:
    - usage_percentage (int): Percentage of ambulances in use.
    - station_name (str): Name of the station.
    - color_scheme (str): Color scheme for the chart.

    Returns:
    - alt.Chart object: Donut chart representation.
    """
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
        color=alt.Color("Status:N", scale=alt.Scale(range=colors[color_scheme]), legend=alt.Legend(title="Ambulance Status")),
        tooltip=["Status:N", "Value:Q"]
    ).properties(width=130, height=130, title=station_name)

# Function to load animated map HTML
def load_map_html(filepath="animated_map.html"):
    """
    Loads an animated map from an HTML file.

    Parameters:
    - filepath (str): Path to the HTML file.

    Returns:
    - str: HTML content of the map.
    """
    with open(filepath, "r") as f:
        return f.read()

# Function to create and display the ambulance data table
def display_ambulance_data():
    """
    Displays the ambulance availability table in Streamlit.
    """
    st.subheader("Ambulance station availability")

    # Sample data representing station capacities and availability
    ambulance_data = pd.DataFrame({
        "Station": ["Maarssen", "Vader Rijndreef", "Diakonessenhuis", "On the move"],
        "Ambulances": [3, 5, 2, 5],
        "Capacity": [5, 6, 4, 8]
    })

    ambulance_data["In Use"] = ambulance_data["Capacity"] - ambulance_data["Ambulances"]

    # Display the table with customized columns
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

# Function to display ambulance availability charts
def display_availability_charts(ambulance_data):
    """
    Generates and displays donut charts for ambulance availability.

    Parameters:
    - ambulance_data (DataFrame): Data containing ambulance usage stats.
    """
    st.subheader("Ambulance availability")

    color_options = ['red', 'green', 'blue', 'yellow']

    for i, row in ambulance_data.iterrows():
        usage_percentage = int((row["In Use"] / row["Capacity"]) * 100)
        st.altair_chart(make_pie_chart(usage_percentage, row["Station"], color_options[i]), use_container_width=True)


has_marked_train = False
client = redis.Redis(host="tile38", port=9851, decode_responses=True)

async def mark_train_once():
    """ Ensures mark_random_train_as_inactive() is only executed once """
    global has_marked_train
    if not has_marked_train:
        try:
            await mark_random_train_as_inactive(client)  # Must be inside an async function
            has_marked_train = True  # Set flag to prevent re-execution
            logging.info("✅ Marked one random train as inactive!")
        except Exception as e:
            logging.error(f"[ERROR] mark_random_train_as_inactive() failed: {e}")


# Main dashboard layout setup
def main():
    """
    Initializes and renders the dashboard with sidebar, tables, and charts.
    """
    # Sidebar with dashboard title
    with st.sidebar:
        st.title("RCOP Dashboard")

    # Define layout columns
    col = st.columns((2.5, 5, 1), gap="small")

    # Display ambulance data table in the left column
    with col[0]:
        ambulance_data = display_ambulance_data()
        display_availability_charts(ambulance_data)

    # Display animated map in the middle column
    with col[1]:
        # Run the async function in the event loop
        asyncio.run(mark_train_once())  # Ensures proper async execution
        
        st.components.v1.html(load_map_html(), height=500, scrolling=False)

# Run the dashboard
if __name__ == "__main__":
    main()
