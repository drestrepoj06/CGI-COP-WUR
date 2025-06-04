import requests
import json
import time
from datetime import datetime

API_KEY = "43846b8fddcf4e2bb41def8291de6bf4"

params = {
    "lat": 52.1100,
    "lng": 5.1253,
    "radius": 150000  # in meters (150 km)
}
headers = {
    "Ocp-Apim-Subscription-Key": API_KEY
}

SAVE_PATH = "trains_log.json"
duration_minutes = 10
interval_seconds = 5
end_time = time.time() + duration_minutes * 60

all_data = []

while time.time() < end_time:
    try:
        response = requests.get("https://gateway.apiportal.ns.nl/virtual-train-api/vehicle", headers=headers, params=params)
        response.raise_for_status()
        trains = response.json().get("payload", {}).get("treinen", [])
        timestamp = datetime.utcnow().isoformat()

        all_data.append({
            "timestamp": timestamp,
            "trains": trains
        })

        # Save updated list to file
        with open(SAVE_PATH, "w") as f:
            json.dump(all_data, f, indent=2)

        print(f"[{timestamp}] Collected {len(trains)} trains")

    except Exception as e:
        print(f"Error: {e}")

    time.sleep(interval_seconds)
