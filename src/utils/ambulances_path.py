import time
import json
from datetime import datetime, timezone
import math
import random
import os
import requests

# Configuration
script_dir = os.path.dirname(os.path.abspath(__file__))
geojson_path = os.path.join(script_dir, 'AmbulanceStations.geojson')
SAVE_PATH = os.path.join(script_dir, 'ambulance_logs.json')
update_interval = 1  # seconds between updates
simulation_minutes = 10
AVERAGE_SPEED_KPH = 50  # Average speed of ambulances in km/h

# TOMTOM API
TOMTOM_API_KEY = "qVEH8Wgf8a02QXAKbeGFxawLEALIefZJ"

# Load stations
with open(geojson_path) as f:
    stations_data = json.load(f)
station_coords = [(f['geometry']['coordinates'][1], f['geometry']['coordinates'][0])
                 for f in stations_data['features']]

# Coordinate lists
destinations = [
    (52.113716, 5.145675), (52.094442, 5.092170),
    (52.089986, 5.101651), (52.057431, 5.133380),
    (52.128689, 5.035797), (52.057080, 5.074981),
    (52.108185, 5.173484)
]

ambulance_origins = [
    (52.077325, 5.026675), (52.117976, 5.114571),
    (52.108185, 5.173484), (52.054173, 5.078710),
    (52.084395, 5.160647), (52.078915, 5.081251),
    (52.109697, 5.014575), (52.091064, 5.083252),
    (52.058347, 5.135314)
]

def get_route_points(origin, destination):
    """Get route points from TOMTOM API"""
    url = (
        f"https://api.tomtom.com/routing/1/calculateRoute/"
        f"{origin[0]},{origin[1]}:{destination[0]},{destination[1]}/json"
        f"?key={TOMTOM_API_KEY}&travelMode=car&routeType=fastest"
    )
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r.json()["routes"][0]["legs"][0]["points"]
    except Exception as e:
        print(f"Error fetching route: {e}")
        return []

def calculate_bearing(p1, p2):
    """Calculate bearing between two points"""
    lat1, lon1 = map(math.radians, p1)
    lat2, lon2 = map(math.radians, p2)
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    bearing = math.atan2(x, y)
    return (math.degrees(bearing) + 360) % 360

def get_initial_availability(is_stationary):
    """Determine initial availability based on ambulance type"""
    if is_stationary:
        return random.random() < 0.7  # 70% chance for stationary to be available
    return random.random() < 0.4  # 40% chance for mobile to be available

# Initialize ambulances
ambulances = []

# Mobile ambulances first (IDs 1-9)
for i, origin in enumerate(ambulance_origins):
    destination = random.choice([d for d in destinations if d != origin])
    route_points = get_route_points(origin, destination)
    ambulances.append({
        "id": f"{i+1}",
        "current_pos": origin,
        "destination": destination,
        "is_stationary": False,
        "station_name": "On the move",
        "availability_status": get_initial_availability(False),
        "speed_kph": random.uniform(AVERAGE_SPEED_KPH-10, AVERAGE_SPEED_KPH+10),
        "route_points": route_points,
        "route_index": 0,
        "last_update": time.time()
    })

# Stationary ambulances (IDs 10+)
while len(ambulances) < 17:
    station_index = random.randint(0, len(station_coords) - 1)
    station = station_coords[station_index]
    ambulances.append({
        "id": f"{len(ambulances) + 1}",
        "current_pos": station,
        "destination": None,
        "is_stationary": True,
        "station_name": stations_data['features'][station_index]['properties']['StationName'],
        "availability_status": get_initial_availability(True),
        "speed_kph": 0,
        "heading": 0,
        "route_points": [],
        "route_index": 0
    })

# Main simulation loop
start_time = time.time()
log_data = []

while time.time() < start_time + simulation_minutes * 60:
    timestamp = datetime.now(timezone.utc).isoformat()
    ambulance_snapshots = []
   
    for ambulance in ambulances:
        if ambulance["is_stationary"]:
            # Stationary ambulance - always include in snapshot
            ambulance_snapshots.append({
                "id": ambulance["id"],
                "lat": ambulance["current_pos"][0],
                "lng": ambulance["current_pos"][1],
                "speed_kph": 0,
                "heading": 0,
                "availability_status": ambulance["availability_status"],
                "is_stationary": True,
                "station_name": ambulance["station_name"],
                "destination": None
            })
        else:
            # Mobile ambulance logic
            route_points = ambulance["route_points"]
            route_index = ambulance["route_index"]
           
            if not route_points or route_index >= len(route_points) - 1:
                # Get new destination and route
                new_dest = random.choice([d for d in destinations if d != ambulance["current_pos"]])
                print(f"{timestamp} - {ambulance['id']} needs new route to {new_dest}")
                ambulance["destination"] = new_dest
                ambulance["route_points"] = get_route_points(ambulance["current_pos"], new_dest)
                ambulance["route_index"] = 0
                ambulance["speed_kph"] = random.uniform(AVERAGE_SPEED_KPH-10, AVERAGE_SPEED_KPH+10)
                continue
           
            # Move to next point in route
            ambulance["route_index"] += 1
            new_point = route_points[ambulance["route_index"]]
            prev_point = route_points[ambulance["route_index"] - 1]
           
            # Update position
            ambulance["current_pos"] = (new_point["latitude"], new_point["longitude"])
           
            # Calculate bearing
            bearing = calculate_bearing(
                (prev_point["latitude"], prev_point["longitude"]),
                (new_point["latitude"], new_point["longitude"])
            )
           
            ambulance_snapshots.append({
                "id": ambulance["id"],
                "lat": new_point["latitude"],
                "lng": new_point["longitude"],
                "speed_kph": round(ambulance["speed_kph"], 1),
                "heading": round(bearing, 1),
                "availability_status": ambulance["availability_status"],
                "is_stationary": False,
                "station_name": "On the move",
                "destination": ambulance["destination"]
            })
   
    # Create the log entry with all ambulances
    log_data.append({
        "timestamp": timestamp,
        "ambulances": ambulance_snapshots
    })
   
    # Save every 10 updates
    if len(log_data) % 10 == 0:
        with open(SAVE_PATH, "w") as f:
            json.dump(log_data, f, indent=2)
   
    print(f"[{timestamp}] Updated {len([a for a in ambulances if a['is_stationary']])} stationary and {len([a for a in ambulances if not a['is_stationary']])} mobile ambulances")
    time.sleep(update_interval)

# Final save
with open(SAVE_PATH, "w") as f:
    json.dump(log_data, f, indent=2)
print(f"✅ Simulation complete! Data saved to {SAVE_PATH}")
