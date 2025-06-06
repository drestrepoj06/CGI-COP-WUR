import time
import json
from datetime import datetime
import requests
import math
import random

TOMTOM_API_KEY = "qVEH8Wgf8a02QXAKbeGFxawLEALIefZJ"
SAVE_PATH = "ambulance_logs.json"
interval_seconds = 5
run_minutes = 1

ambulance_origins = [
    (52.121274, 5.035082),
    (52.108948, 5.114417),
    (52.080973, 5.137714),
]
destination = (52.1100, 5.1253)

def get_route_points(origin, destination):
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
    lat1, lon1 = map(math.radians, p1)
    lat2, lon2 = map(math.radians, p2)
    dlon = lon2 - lon1
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    bearing = math.atan2(x, y)
    return (math.degrees(bearing) + 360) % 360

# Precompute routes
routes = [get_route_points(o, destination) for o in ambulance_origins]
route_indices = [0] * len(routes)

log_data = []
end_time = time.time() + run_minutes * 60

while time.time() < end_time:
    timestamp = datetime.utcnow().isoformat()
    ambulance_snapshots = []

    for i, route in enumerate(routes):
        if not route or route_indices[i] >= len(route):
            continue

        # Advance position
        route_indices[i] = min(route_indices[i] + 1, len(route) - 1)
        idx = route_indices[i]
        curr = route[idx]
        prev = route[idx - 1] if idx > 0 else curr

        lat, lng = curr["latitude"], curr["longitude"]
        bearing = calculate_bearing((prev["latitude"], prev["longitude"]), (lat, lng))
        speed = round(random.uniform(30, 70), 2)

        ambulance_snapshots.append({
            "id": f"ambulance_{i+1}",
            "lat": lat,
            "lng": lng,
            "speed_kmh": speed,
            "heading": round(bearing, 2),
            "accuracy_m": round(random.uniform(5, 15), 1),
            "type": "ambulance",
            "source": "simulated"
        })

    log_data.append({
        "timestamp": timestamp,
        "ambulances": ambulance_snapshots
    })

    with open(SAVE_PATH, "w") as f:
        json.dump(log_data, f, indent=2)

    print(f"[{timestamp}] Updated {len(ambulance_snapshots)} ambulances")
    time.sleep(interval_seconds)

print(f"✅ Done! File saved to: {SAVE_PATH}")