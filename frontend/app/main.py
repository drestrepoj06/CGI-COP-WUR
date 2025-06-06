# Import packages
import streamlit as st
import pydeck as pdk
import pandas as pd
import geopandas as gpd
import numpy as np
import json
import time
import math

# Set wide layout
st.set_page_config(layout="wide")

# Set title
st.title("Dashboard with Mapbox-kaart via Pydeck")

# Load GeoJSON with railway lines
rails_gpd = gpd.read_file("rails.geojson")

# Pydeck requires 'path' as list of [lon, lat] pairs
rails_gpd['path'] = rails_gpd['geometry'].apply(lambda line: [[coord[0], coord[1]] for coord in line.coords])

# Pydeck layer with rail lines
rail_layer = pdk.Layer(
    'PathLayer',
    data=rails_gpd,
    get_path='path',
    get_width=4,
    get_color='[255, 0, 0]',
    pickable=True,
    width_min_pixels=2,
)

# Load TomTom route JSON
with open("UtrechtTomTomRoute.json") as f:
    tomtom_data = json.load(f)

# Extract coordinates from TomTom API response
points = tomtom_data['routes'][0]['legs'][0]['points']
points_df = pd.DataFrame(points)
route_path = [[pt['longitude'], pt['latitude']] for pt in points]

# Create DataFrame for the route
route_df = pd.DataFrame({'path': [route_path]})

# Create Pydeck PathLayer for the route
route_layer = pdk.Layer(
    'PathLayer',
    data=route_df,
    get_path='path',
    get_width=6,
    get_color='[0, 0, 255]',  # Blue
    pickable=True,
    width_min_pixels=3,
)

# Create a ScatterplotLayer for the route points
route_points_layer = pdk.Layer(
    "ScatterplotLayer",
    data=points_df,
    get_position='[longitude, latitude]',
    get_fill_color='[0, 255, 0, 160]',  # Blue with some transparency
    get_radius=20,
    pickable=True,
)

# Set starting view state
view_state = pdk.ViewState(
    latitude=52.10436,
    longitude=5.114589,
    zoom=11,
    pitch=0,
)

#%%
def haversine(coord1, coord2):
    # Coordinates in decimal degrees (e.g. 52.1, 5.1)
    lat1, lon1 = coord1
    lat2, lon2 = coord2

    R = 6371000  # Radius of Earth in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)

    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2) ** 2

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c  # Output: distance in meters

# Calculate total time and distance
total_time = tomtom_data['routes'][0]['summary']['travelTimeInSeconds']
total_distance = tomtom_data['routes'][0]['summary']['lengthInMeters']  # meters

# Estimate average speed (meters/second)
speed_mps = 10 * total_distance / total_time

# Container to update chart dynamically
chart_placeholder = st.empty()

# Move point along path
for i in range(len(points) - 1):
    start = points[i]
    end = points[i + 1]

    # Distance between current point and next point (in meters)
    dist = haversine((start['latitude'], start['longitude']),
                    (end['latitude'], end['longitude']))

    # Time to wait between this and next point
    delay = dist / speed_mps

    # Point to draw
    current_point_df = pd.DataFrame([{
        "latitude": start['latitude'],
        "longitude": start['longitude']
    }])

    # Moving point layer
    moving_point_layer = pdk.Layer(
        "ScatterplotLayer",
        data=current_point_df,
        get_position='[longitude, latitude]',
        get_fill_color='[255, 165, 0, 200]',  # Orange
        get_radius=60,
    )

    # Update chart with the moving point
    deck = pdk.Deck(
        layers=[rail_layer, route_layer, route_points_layer, moving_point_layer],
        initial_view_state=view_state,
        map_provider='mapbox',
        map_style='mapbox://styles/mapbox/light-v9',
    )
    chart_placeholder.pydeck_chart(deck)

    time.sleep(delay)