import json
import os
from kafka import KafkaConsumer
from pprint import pprint

# Create Kafka consumer configured for your vehicle data

# consumer = KafkaConsumer(
#     'sim-vehicle',  # Same topic as producer
#     bootstrap_servers=['localhost:9092'],
#     # bootstrap_servers=['kafka:9092'],
#     auto_offset_reset='earliest',  # Start from earliest message
#     value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#     consumer_timeout_ms=5000  # Timeout if no messages for 5 seconds
# )


consumer = KafkaConsumer(
    'sim-vehicle',
    bootstrap_servers=["kafka-server:9092"],  # Match advertised.listeners
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)



# consumer = KafkaConsumer(
#     'sim-vehicle',
#     bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),  # 使用环境变量或直接写死
#     auto_offset_reset='earliest',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#     consumer_timeout_ms=5000
# )

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
    print("Starting vehicle data consumer...")
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
