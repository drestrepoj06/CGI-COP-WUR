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

# Load rails GeoJSON
with open("rails.geojson", "r") as f:
    rails_geojson = json.load(f)

# Convert to JSON string (JavaScript-friendly)
rails_js = json.dumps(rails_geojson)

# Save coordinates for JS
route = [(pt['latitude'], pt['longitude']) for pt in points]
st.session_state['route'] = route  # Optional: if needed later

# Write route to JS file
with open("animated_map.html", "r") as f:
    html = f.read()

# Inject coordinates into placeholder in the HTML
coord_js = str(route).replace("(", "[").replace(")", "]")  # JS-friendly format
html = html.replace("//__INSERT_ROUTE_HERE__", f"const route = {coord_js};")

# Inject rails data
html = html.replace("//__INSERT_RAILS_HERE__", f"const railsData = {rails_js};")

# Show the custom animated map
st.components.v1.html(html, height=600, scrolling=False)
