import json
import time
import logging
import requests
import threading
from kafka import KafkaProducer
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Kafka producer serializer: convert dict to JSON bytes
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Create Kafka producer instance (use your kafka address here)
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],  # Or localhost:9092, adjust for your environment
    value_serializer=serializer
)

# --- Part 1: Train location producer ---

TRAIN_LOGS_PATH = 'utils/train_logs.json'

def load_train_logs():
    with open(TRAIN_LOGS_PATH, 'r') as f:
        return json.load(f)

def parse_timestamp(ts_str):
    dt = datetime.fromisoformat(ts_str)
    return int(dt.timestamp() * 1000)

def produce_train_messages():
    try:
        while True:
            train_logs = load_train_logs()
            for entry in train_logs:
                timestamp = parse_timestamp(entry['timestamp'])
                for train in entry['trains']:
                    message = {
                        "timestamp": timestamp,
                        "type": "train",
                        "vehicle_id": str(train.get("ritId", train.get("treinNummer"))),
                        "train_number": train.get("treinNummer"),
                        "speed": train.get("snelheid"),
                        "direction": train.get("richting"),
                        "latitude": train.get("lat"),
                        "longitude": train.get("lng"),
                        "train_type": train.get("type"),
                        "source": train.get("bron")
                    }
                    logging.info(f"Sending train data: {message}")
                    producer.send('train-locations', message)
                producer.flush()
                time.sleep(5)  # Wait 5 seconds between batches
    except Exception as e:
        logging.error(f"Train producer error: {e}")

# --- Part 2: Vehicle route producer with TomTom API ---

vehicles_config = {
    "vehicle1": {"mode": "route", "speed": 200.0, "location": [[51.895532, 4.468462]]},
    "vehicle2": {"mode": "route", "speed": 250.0, "location": [[51.895373, 4.485959]]},
    "vehicle3": {"mode": "route", "speed": 200.0, "location": [[51.897227, 4.495136]]},
}

TOMTOM_API_KEY = "qVEH8Wgf8a02QXAKbeGFxawLEALIefZJ"  # Replace with your key

def get_route_from_tomtom(origin, destination, traffic=True):
    base_url = "https://api.tomtom.com/routing/1/calculateRoute"
    origin_str = f"{origin[0]},{origin[1]}"
    destination_str = f"{destination[0]},{destination[1]}"
    traffic_mode = "traffic:enabled" if traffic else "traffic:disabled"

    url = (
        f"{base_url}/{origin_str}:{destination_str}/json"
        f"?key={TOMTOM_API_KEY}"
        f"&travelMode=car"
        f"&routeType=fastest"
        f"&traffic={traffic_mode}"
        f"&computeBestOrder=false"
        f"&sectionType=travelMode"
        f"&instructionType=text"
        f"&report=effectiveSettings"
    )

    try:
        response = requests.get(url)
        response.raise_for_status()
        route_data = response.json()
        summary = route_data['routes'][0]['summary']
        return {
            "lengthInMeters": summary['lengthInMeters'],
            "travelTimeInSeconds": summary['travelTimeInSeconds'],
            "trafficDelayInSeconds": summary.get('trafficDelayInSeconds', 0),
            "geometry": route_data['routes'][0]['legs'][0]['points'],
        }
    except requests.RequestException as e:
        logging.error(f"Failed to get route from TomTom: {e}")
        return None

def produce_vehicle_messages():
    destination = (51.9, 4.5)  # Example fixed destination
    try:
        while True:
            for vehicle_id, vehicle_data in vehicles_config.items():
                origin = vehicle_data['location'][0]
                route = get_route_from_tomtom(origin, destination, traffic=True)
                if route:
                    message = {
                        "timestamp": int(time.time() * 1000),
                        "type": "vehicle",
                        "vehicle_id": vehicle_id,
                        "mode": vehicle_data['mode'],
                        "speed": vehicle_data['speed'],
                        "origin": origin,
                        "destination": destination,
                        "route_summary": {
                            "lengthInMeters": route["lengthInMeters"],
                            "travelTimeInSeconds": route["travelTimeInSeconds"],
                            "trafficDelayInSeconds": route["trafficDelayInSeconds"],
                        },
                        "route_geometry": route["geometry"]
                    }
                    logging.info(f"Sending vehicle route data: {message}")
                    producer.send('sim-vehicle', message)
                else:
                    logging.warning(f"Skipping message for {vehicle_id} due to route fetch failure.")
            producer.flush()
            time.sleep(2)  # Wait 2 seconds before next batch
    except Exception as e:
        logging.error(f"Vehicle producer error: {e}")

# --- Main: start both producers in threads ---

def main():
    train_thread = threading.Thread(target=produce_train_messages, daemon=True)
    vehicle_thread = threading.Thread(target=produce_vehicle_messages, daemon=True)

    train_thread.start()
    vehicle_thread.start()

    try:
        while True:
            time.sleep(1)  # Keep main thread alive
    except KeyboardInterrupt:
        logging.info("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
