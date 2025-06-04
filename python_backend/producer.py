import json
import time
import logging
import requests
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Vehicle configuration
vehicles_config = {
    "vehicle1": {
        "mode": "route",
        "speed": 200.0,
        "location": [
            [51.895532, 4.468462],
        ],
    },
    "vehicle2": {
        "mode": "route",
        "speed": 250.0,
        "location": [
            [51.895373, 4.485959],
        ],
    },
    "vehicle3": {
        "mode": "route",
        "speed": 200.0,
        "location": [
            [51.897227, 4.495136],
        ],
    },
}

# TomTom API key
TOMTOM_API_KEY = "qVEH8Wgf8a02QXAKbeGFxawLEALIefZJ"  # Replace with your API key

def serializer(message):
    """Convert messages to JSON format for Kafka"""
    return json.dumps(message).encode('utf-8')

# Create Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Change if needed
    value_serializer=serializer
)

def get_route_from_tomtom(origin, destination, traffic=True):
    """Fetch route data from TomTom Routing API."""
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

def generate_vehicle_messages():
    """Generate and send vehicle data messages with TomTom routing info."""
    for vehicle_id, vehicle_data in vehicles_config.items():
        origin = vehicle_data['location'][0]
        # Example destination - customize as needed
        destination = (51.9, 4.5)

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

def main():
    """Main loop that runs the Kafka producer."""
    try:
        while True:
            generate_vehicle_messages()
            time.sleep(2)  # Send every 2 seconds
    except KeyboardInterrupt:
        logging.info("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()