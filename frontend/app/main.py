import streamlit as st
import json
import pandas as pd

st.set_page_config(layout="wide")
st.title("Animated Route with Custom Leaflet + JavaScript")

# Load TomTom route
with open("UtrechtTomTomRoute.json") as f:
    data = json.load(f)

points = data['routes'][0]['legs'][0]['points']
points_df = pd.DataFrame(points)

# Save coordinates for JS
coords = [(pt['latitude'], pt['longitude']) for pt in points]
st.session_state['coords'] = coords  # Optional: if needed later

# Write coords to JS file
with open("animated_map.html", "r") as f:
    html = f.read()

# Inject coordinates into placeholder in the HTML
coord_js = str(coords).replace("(", "[").replace(")", "]")  # JS-friendly format
html = html.replace("//__INSERT_COORDS_HERE__", f"const route = {coord_js};")

# Show the custom animated map
st.components.v1.html(html, height=600, scrolling=False)
