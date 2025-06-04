import os
import pandas as pd
import sqlite3
import json
from collections import defaultdict

# === Set your base path ===
base_path = "/Users/mate/CGI-COP-WUR/Galgenwaard_Junction_2025-06-04_2025-06-04"

csv_folder = os.path.join(base_path, "csv")

definition_path = os.path.join(csv_folder, "2025_06_04_07_40_37_definition.csv")
approaches_path = os.path.join(csv_folder, "approaches.csv")
turn_ratios_path = os.path.join(csv_folder, "turn_ratios.csv")
# Read CSVs into DataFrames
definition_df = pd.read_csv(definition_path)
approaches_df = pd.read_csv(approaches_path)
turn_ratios_df = pd.read_csv(turn_ratios_path)

# Create nested structure
junctions = defaultdict(lambda: {
    "junctionName": "",
    "countryCode": "",
    "driveOnLeft": None,
    "location": "",
    "radius": None,
    "approaches": {}
})

# Populate junction and approach metadata
for _, row in definition_df.iterrows():
    junc_id = row["junctionId"]
    approach_id = str(row["id"])

    junctions[junc_id]["junctionName"] = row["junctionName"]
    junctions[junc_id]["countryCode"] = row["countryCode"]
    junctions[junc_id]["driveOnLeft"] = row["driveOnLeft"]
    junctions[junc_id]["location"] = row["rawJunctionGeometryWKT"]
    junctions[junc_id]["radius"] = row["rawJunctionRadius"]

    if row["type"] == "APPROACH":
        junctions[junc_id]["approaches"][approach_id] = {
            "name": row["name"],
            "roadName": row["roadName"],
            "direction": row["direction"],
            "travelData": [],
            "turnRatios": []
        }

# Append time-series travel data to approaches
for _, row in approaches_df.iterrows():
    junc_id = row["junctionId"]
    approach_id = str(row["approachId"])

    if approach_id in junctions[junc_id]["approaches"]:
        junctions[junc_id]["approaches"][approach_id]["travelData"].append({
            "time": row["time"],
            "travelTimeSec": row["travelTimeSec"],
            "freeFlowTravelTimeSec": row["freeFlowTravelTimeSec"],
            "delaySec": row["delaySec"],
            "usualDelaySec": row["usualDelaySec"],
            "stops": row["stops"],
            "queueLengthMeters": row["queueLengthMeters"],
            "volumePerHour": row["volumePerHour"],
            "isClosed": row["isClosed"],
            "stopsHistogram": row["stopsHistogram"]
        })

# Append turn ratio data to approaches
for _, row in turn_ratios_df.iterrows():
    junc_id = row["junctionId"]
    approach_id = str(row["approachId"])

    if approach_id in junctions[junc_id]["approaches"]:
        junctions[junc_id]["approaches"][approach_id]["turnRatios"].append({
            "time": row["time"],
            "exitId": row["exitId"],
            "exitIndex": row["exitIndex"],
            "ratioPercent": row["ratioPercent"],
            "probesCount": row["probesCount"]
        })

# Convert to JSON and write to file
with open("combined_junction_data.json", "w") as f:
    json.dump(junctions, f, indent=2)