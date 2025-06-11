import requests
import json
import time
from datetime import datetime
from geopy.distance import geodesic

API_KEY = "43846b8fddcf4e2bb41def8291de6bf4"
CENTER = (52.094040, 5.093102)  
MAX_DIST_km = 10

headers = {
    "Ocp-Apim-Subscription-Key": API_KEY
}
SAVE_PATH = "train_logs.json"
duration_minutes = 10
interval_seconds = 5
end_time = time.time() + duration_minutes * 60

all_data = []

def is_within_radius(train):
    coords = train.get("lat"), train.get("lng")
    if None in coords:
        return False
    return geodesic(CENTER, coords).km <= MAX_DIST_km

while time.time() < end_time:
    try:
        response = requests.get("https://gateway.apiportal.ns.nl/virtual-train-api/vehicle", headers=headers)
        response.raise_for_status()
        all_trains = response.json().get("payload", {}).get("treinen", [])
        nearby_trains = [t for t in all_trains if is_within_radius(t)]
        timestamp = datetime.utcnow().isoformat()

        all_data.append({
            "timestamp": timestamp,
            "trains": nearby_trains
        })

        with open(SAVE_PATH, "w") as f:
            json.dump(all_data, f, indent=2)

        print(f"[{timestamp}] Collected {len(nearby_trains)} nearby trains")

    except Exception as e:
        print(f"Error: {e}")

    time.sleep(interval_seconds)
