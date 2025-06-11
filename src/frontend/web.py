import streamlit as st
import json
import os
import pandas as pd
from glob import glob
import altair as alt


# Load TomTom route
with open("frontend/UtrechtTomTomRoute.json") as f:
    data = json.load(f)

points = data['routes'][0]['legs'][0]['points']
points_df = pd.DataFrame(points)

# Save coordinates for JS
route = [(pt['latitude'], pt['longitude']) for pt in points]
st.session_state['route'] = route  # Optional: if needed later

# st.set_page_config(layout="wide")
# st.title("Live Train Animation Map")


# Load rails GeoJSON
with open("frontend/rails.geojson", "r") as f:
    rails_geojson = json.load(f)
rails_js = json.dumps(rails_geojson)

# Load and sort all train location files
train_files = sorted(
    glob("frontend/data/train_location_data/*.json"),
    key=lambda f: int(os.path.basename(f).replace(".json", "").split("_")[1])
)


# Parse all train points, keyed by timestamp
train_data = []
timestamps = []

for file in train_files:
    timestamp = int(os.path.basename(file).replace(".json", "").split("_")[1])
    with open(file, "r") as f:
        geojson = json.load(f)

    features = [
        {
            "lat": f["geometry"]["coordinates"][1],
            "lon": f["geometry"]["coordinates"][0],
            "ritId": f["properties"]["ritId"],
            "speed": f["properties"]["speed"],
            "type": f["properties"]["type"]
        }
        for f in geojson["features"]
    ]

    train_data.append({
        "timestamp": timestamp,
        "features": features
    })
    timestamps.append(timestamp)

# Prepare JS-friendly object for train data
train_js_data = json.dumps(train_data)

# Load and sort all ambulance location files
ambulance_files = sorted(
    glob("frontend/data/ambulance_location_data/*.json"),
    key=lambda f: int(os.path.basename(f).replace(".json", "").split("_")[1])  
)

ambulance_data = []

for file in ambulance_files:
    # Extract just the numeric part of the filename
    timestamp = int(os.path.basename(file).replace(".json", "").split("_")[1])
    with open(file, "r") as f:
        geojson = json.load(f)

    features = [
        {
            "lat": f["geometry"]["coordinates"][1],
            "lon": f["geometry"]["coordinates"][0],
            "vehicle_number": f["properties"]["vehicle_number"],
            "speed": f["properties"]["speed"],
            "type": f["properties"]["type"]
        }
        for f in geojson["features"]
    ]

    ambulance_data.append({
        "timestamp": timestamp,
        "features": features
    }
    )

# Prepare JS-friendly object for ambulance data
ambulance_js_data = json.dumps(ambulance_data)

# Donut chart generator
def make_pie(input_response, input_text, input_color):
    if input_color == 'blue':
        chart_color = ['#29b5e8', '#155F7A']
    elif input_color == 'green':
        chart_color = ['#27AE60', '#12783D']
    elif input_color == 'red':
        chart_color = ['#E74C3C', '#781F16']
    
    source = pd.DataFrame({
        "Status": ['Available', 'In Use'],
        "Value": [100 - input_response, input_response],
    })

    plot = alt.Chart(source).mark_arc().encode(
        theta="Value:Q",
        color=alt.Color("Status:N",
                        scale=alt.Scale(range=chart_color),
                        legend=alt.Legend(title="Ambulance Status")),
        tooltip=["Status:N", "Value:Q"]
    ).properties(width=130, height=130, title=input_text)
    
    return plot

# Inject into HTML
with open("frontend/animated_map.html", "r") as f:
    html = f.read()

coord_js = str(route).replace("(", "[").replace(")", "]")  # JS-friendly format
html = html.replace("//__INSERT_ROUTE_HERE__", f"const route = {coord_js};")
html = html.replace("//__INSERT_TRAIN_DATA_HERE__", f"const trainData = {train_js_data};\nconst ambulanceData = {ambulance_js_data};")
html = html.replace("//__INSERT_RAILS_HERE__", f"const railsData = {rails_js};")


# Streamlit app setup
st.set_page_config(
    page_title="COP Dashboard",
    page_icon="🚅",
    layout="wide",
    initial_sidebar_state="collapsed",)

# Sidebar
with st.sidebar:
    st.title('COP dashboard')

# Create columns for layout
col = st.columns((2.5, 5, 1), gap='small')

# Left column
with col[0]:
    # Ambulance data table
    st.subheader("Ambulance station availability")

    # Sample data
    ambulance_data = pd.DataFrame({
        "Station": ["Maarssen", "Vader Rijndreef", "Diakonessenhuis", "On the move"],
        "Ambulances": [3, 5, 2, 5],
        "Capacity": [5, 6, 4, 8]
    })

    ambulance_data["In Use"] = ambulance_data["Capacity"] - ambulance_data["Ambulances"]
    
    # Display the table
    st.dataframe(
        ambulance_data,
        column_order=("Station", "Ambulances"),
        hide_index=True,
        width=None,
        column_config={
            "Station": st.column_config.TextColumn(
                "Station",
                width="small"
            ),
            "Ambulances": st.column_config.ProgressColumn(
                "Available",
                format="%d",  # Display as integer
                min_value=0,
                max_value=max(ambulance_data["Capacity"]),
                width="small"
            )
        }
    )    
    
    st.subheader("Ambulance availability")
    
    colors = ['red', 'green', 'blue']
    
    for i, row in ambulance_data.iterrows():
        pct = int((row["In Use"] / row["Capacity"]) * 100)
        st.altair_chart(make_pie(pct, row["Station"], colors[i]), use_container_width=True)


# Middle column
with col[1]:
    st.components.v1.html(html, height=700, scrolling=False) # Show the animated map

# Right column
with col[2]:
    st.button("Button 1")
    st.button("Button 2")
    st.button("Button 3")

