import time
import sys
import json
import logging
import requests
import threading
from datetime import datetime

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError


def create_topics(admin_client):
    """Create topic lists"""
    print("⏳ Creating Kafka topics...")
    topic_list = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in TOPICS]
    
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"✅ Successfully created {len(TOPICS)} topics: {', '.join(TOPICS)}")
    except TopicAlreadyExistsError:
        print(f"ℹ️ Topics already exist, skipping creation: {', '.join(TOPICS)}")
    except Exception as e:
        print(f"❌ Failed to create topics: {e}", file=sys.stderr)
        sys.exit(1)

def produce_test_messages():
    """Produce test messages to all topics"""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        api_version=(3, 6, 0)
    )
    
    for topic in TOPICS:
        try:
            for i in range(3):  # Send 3 messages to each topic
                message = f"Test message {i} - {time.strftime('%H:%M:%S')}".encode('utf-8')
                producer.send(topic, message)
                producer.flush()
                print(f"📤 Sent to [{topic}]: {message.decode()}")
        except KafkaError as e:
            print(f"❌ Producer error ({topic}): {e}", file=sys.stderr)
    
    producer.close()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
TOPICS = ['train-locations', 'sim-tomtom-vehicle']

# TomTom API configuration
TOMTOM_API_KEY = "qVEH8Wgf8a02QXAKbeGFxawLEALIefZJ"  # Replace with your key

# # Vehicle configuration
# vehicles_config = {
#     "vehicle1": {"mode": "route", "speed": 200.0, "location": [[51.895532, 4.468462]]},
#     "vehicle2": {"mode": "route", "speed": 250.0, "location": [[51.895373, 4.485959]]},
#     "vehicle3": {"mode": "route", "speed": 200.0, "location": [[51.897227, 4.495136]]},
# }

def create_kafka_admin(max_retries=5, retry_interval=5):
    """Create Kafka admin client with retry mechanism"""
    for i in range(max_retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=[KAFKA_BROKER],
                api_version=(3, 0, 0)
            )
            logging.info(f"✅ Successfully connected to Kafka Broker: {KAFKA_BROKER}")
            return admin
        except Exception as e:
            if i == max_retries - 1:
                logging.error(f"❌ Failed to create KafkaAdminClient: {e}")
                raise
            logging.warning(f"⚠️ Connection failed ({i+1}/{max_retries}), retrying in {retry_interval}s...")
            time.sleep(retry_interval)

def ensure_topics_exist(admin_client=None):
    """Ensure required Kafka topics exist"""
    try:
        if admin_client is None:
            admin_client = create_kafka_admin()
            
        existing_topics = admin_client.list_topics()
        topics_to_create = [t for t in TOPICS if t not in existing_topics]
        
        if topics_to_create:
            topic_list = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics_to_create]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logging.info(f"✅ Created {len(topic_list)} topics: {', '.join(topics_to_create)}")
        else:
            logging.info(f"ℹ️ All topics already exist: {', '.join(TOPICS)}")
            
        return admin_client
    except TopicAlreadyExistsError:
        logging.info(f"ℹ️ Topics already exist: {', '.join(TOPICS)}")
    except Exception as e:
        logging.error(f"❌ Failed to create topics: {e}")
        raise
    finally:
        if admin_client:
            admin_client.close()

def create_kafka_producer():
    """Create Kafka producer instance"""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        api_version=(3, 0, 0),
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

TRAIN_LOGS_PATH = 'utils/train_logs.json'

def load_train_logs():
    """Load train log data from JSON file"""
    with open(TRAIN_LOGS_PATH, 'r') as f:
        return json.load(f)

def parse_timestamp(ts_str):
    """Convert ISO timestamp to milliseconds"""
    dt = datetime.fromisoformat(ts_str)
    return int(dt.timestamp() * 1000)

def produce_train_messages(producer):
    """Produce train location messages"""
    try:
        while True:
            train_logs = load_train_logs()
            for entry in train_logs:
                timestamp = parse_timestamp(entry['timestamp'])
                for train in entry['trains']:
                    message = {
                        "timestamp": timestamp,
                        "type": train['type'],
                        "ritId": train['ritId'],
                        "lat": train['lat'],
                        "lng": train['lng'],
                        "speed": train['snelheid'],
                        "direction": train['richting']
                    }
                    logging.info(f"Sending train data: {message}")
                    producer.send('train-locations', message)
                    logging.info("'train-locations' message sent successfully!")
                producer.flush()
                time.sleep(5)
    except Exception as e:
        logging.error(f"Train producer error: {e}")


# def get_route_from_tomtom(origin, destination, traffic=True):
#     """Get route data from TomTom API"""
#     base_url = "https://api.tomtom.com/routing/1/calculateRoute"
#     origin_str = f"{origin[0]},{origin[1]}"
#     destination_str = f"{destination[0]},{destination[1]}"
#     traffic_mode = "true" if traffic else "false"

#     url = (
#         f"{base_url}/{origin_str}:{destination_str}/json"
#         f"?key={TOMTOM_API_KEY}"
#         f"&travelMode=car"
#         f"&routeType=fastest"
#         f"&traffic={traffic_mode}"
#         f"&computeBestOrder=false"
#         f"&sectionType=travelMode"
#         f"&instructionsType=text"
#         f"&report=effectiveSettings"
#     )

#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         route_data = response.json()
#         summary = route_data['routes'][0]['summary']
#         return {
#             "lengthInMeters": summary['lengthInMeters'],
#             "travelTimeInSeconds": summary['travelTimeInSeconds'],
#             "trafficDelayInSeconds": summary.get('trafficDelayInSeconds', 0),
#             "geometry": route_data['routes'][0]['legs'][0]['points'],
#         }
#     except requests.RequestException as e:
#         logging.error(f"Failed to get route from TomTom: {e}")
#         return None

# def produce_vehicle_messages(producer):
#     """Produce vehicle route messages"""
#     destination = (51.9, 4.5)  # Example fixed destination
#     try:
#         while True:
#             for vehicle_id, vehicle_data in vehicles_config.items():
#                 origin = vehicle_data['location'][0]
#                 route = get_route_from_tomtom(origin, destination, traffic=True)
#                 if route:
#                     message = {"key": "value"}  # Example message format
#                     logging.info(f"Sending vehicle route data: {message}")
#                     producer.send('sim-tomtom-vehicle', message)
#                     logging.info("'sim-tomtom-vehicle' message sent successfully!")
#                 else:
#                     logging.warning(f"Skipping message for {vehicle_id} due to route fetch failure")
#             producer.flush()
#             time.sleep(2)
#     except Exception as e:
#         logging.error(f"Vehicle producer error: {e}")


def main():
    try:
        # 1. Ensure Kafka topics exist # this part works well
        ensure_topics_exist()
        
        # 2. Create producer
        producer = create_kafka_producer()
        
        # 3. Start producer threads
        train_thread = threading.Thread(
            target=produce_train_messages,
            args=(producer,),
            daemon=True
        )
        
        # vehicle_thread = threading.Thread(
        #     target=produce_vehicle_messages,
        #     args=(producer,),
        #     daemon=True
        # )
        
        train_thread.start()
        # vehicle_thread.start()
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Stopping producer...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        if 'producer' in locals():
            producer.close()
        logging.info("Application shutdown complete")

if __name__ == "__main__":
    main()
