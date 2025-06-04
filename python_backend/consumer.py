import json
import os
from kafka import KafkaConsumer
from pprint import pprint

consumer = KafkaConsumer(
    'sim-vehicle',
    bootstrap_servers=["kafka:9092"],  # Match internal listener
    api_version=("2.8.0"),
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def process_vehicle_message(message):
    """Process and display vehicle data"""
    data = message.value
    print("\nReceived Vehicle Update:")
    print(f"Vehicle ID: {data['vehicle_id']}")
    print(f"Type: {data['type']}")
    print(f"Location: {data['latitude']}, {data['longitude']}")
    print(f"Speed: {data['speed']} units")
    print(f"Mode: {data['mode']}")
    print(f"Timestamp: {data['timestamp']}")

def main():
    try:
        for message in consumer:
            if message.value['type'] == 'vehicle':  # Filter for vehicle messages
                process_vehicle_message(message)
    except KeyboardInterrupt:
        print("\nStopping consumer gracefully...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
