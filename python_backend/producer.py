import json
import time
from kafka import KafkaProducer

# Vehicle configuration - this is the data you want to keep
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

def serializer(message):
    """Convert messages to JSON format for Kafka"""
    return json.dumps(message).encode('utf-8')

# Create Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=serializer
)

def generate_vehicle_messages():
    """Generate and send vehicle data messages"""
    for vehicle_id, vehicle_data in vehicles_config.items():
        for location in vehicle_data['location']:
            message = {
                "timestamp": int(time.time() * 1000),
                "type": "vehicle",
                "vehicle_id": vehicle_id,
                "mode": vehicle_data['mode'],
                "speed": vehicle_data['speed'],
                "latitude": location[0],
                "longitude": location[1],
                "coordinates": {
                    "lat": location[0],
                    "lon": location[1]
                }
            }
            print(f"Sending vehicle data: {message}")
            producer.send('sim-vehicle', message)
    
    producer.flush()  # Ensure all messages are sent

def main():
    """Main loop that runs the producer"""
    try:
        while True:
            generate_vehicle_messages()
            time.sleep(2)  # Send data every 2 seconds
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
