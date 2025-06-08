import streamlit as st
import json
import os
import pandas as pd
from glob import glob

st.set_page_config(layout="wide")
st.title("Live Train Animation Map")

# Load rails GeoJSON
with open("frontend/rails.geojson", "r") as f:
    rails_geojson = json.load(f)
rails_js = json.dumps(rails_geojson)


# Load and sort all train location files
train_files = sorted(
    glob("frontend/data/train_location_data/*.json"),
    key=lambda f: int(os.path.basename(f).replace(".json", ""))
)

# Parse all train points, keyed by timestamp
train_data = []
timestamps = []

for file in train_files:
    timestamp = int(os.path.basename(file).replace(".json", ""))
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

# Prepare JS-friendly object
train_js_data = json.dumps(train_data)

# Inject into HTML
with open("frontend/animated_map.html", "r") as f:
    html = f.read()

html = html.replace("//__INSERT_TRAIN_DATA_HERE__", f"const trainData = {train_js_data};")
html = html.replace("//__INSERT_RAILS_HERE__", f"const railsData = {rails_js};")

# Show the animated map
st.components.v1.html(html, height=700, scrolling=False)
