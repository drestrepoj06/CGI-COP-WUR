import time
import sys
import json
import logging
import threading
from datetime import datetime
import redis

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
TOPICS = ['train-locations', 'ambulance-locations']

TRAIN_LOGS_PATH = 'utils/train_logs.json'
AMB_LOGS_PATH = 'utils/ambulance_logs.json'
TOPICS = ['train-locations', 'ambulance-locations']


def create_kafka_admin(max_retries=5, retry_interval=5):
    """Create Kafka admin client with retry mechanism"""
    for i in range(max_retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=[KAFKA_BROKER],
                api_version=(3, 0, 0)
            )
            logging.info(
                f"✅ Successfully connected to Kafka Broker: {KAFKA_BROKER}")
            return admin
        except Exception as e:
            if i == max_retries - 1:
                logging.error(f"❌ Failed to create KafkaAdminClient: {e}")
                raise
            logging.warning(
                f"⚠️ Connection failed ({i+1}/{max_retries}), retrying in {retry_interval}s...")
            time.sleep(retry_interval)


def ensure_topics_exist(admin_client=None):
    """Ensure required Kafka topics exist"""
    try:
        if admin_client is None:
            admin_client = create_kafka_admin()

        existing_topics = admin_client.list_topics()
        topics_to_create = [t for t in TOPICS if t not in existing_topics]

        if topics_to_create:
            topic_list = [NewTopic(
                topic, num_partitions=1, replication_factor=1) for topic in topics_to_create]
            admin_client.create_topics(
                new_topics=topic_list, validate_only=False)
            logging.info(
                f"✅ Created {len(topic_list)} topics: {', '.join(topics_to_create)}")
        else:
            logging.info(f"ℹ️ All topics already exist: {', '.join(TOPICS)}")

    except TopicAlreadyExistsError:
        logging.info(f"ℹ️ Topics already exist: {', '.join(TOPICS)}")
    except Exception as e:
        logging.error(f"❌ Failed to create topics: {e}")
        raise
    finally:
        if admin_client:
            admin_client.close()


def parse_timestamp(ts_str):
    """Convert ISO timestamp string to milliseconds since epoch."""
    dt = datetime.fromisoformat(ts_str)
    return int(dt.timestamp() * 1000)


def load_train_logs():
    """Load train log data from JSON file"""
    try:
        with open(TRAIN_LOGS_PATH, 'r') as f:
            data = json.load(f)
            logging.info(f"Loaded {len(data)} train log entries.")
            return data
    except Exception as e:
        logging.error(f"Failed to load train logs: {e}")
        return []


def load_ambulance_logs():
    """Load ambulance log data from JSON file."""
    try:
        with open(AMB_LOGS_PATH, 'r') as f:
            data = json.load(f)
            logging.info(f"Loaded {len(data)} ambulance log entries.")
            return data
    except Exception as e:
        logging.error(f"Failed to load ambulance logs: {e}")
        return []


def parse_timestamp(ts_str):
    """Convert ISO timestamp string to milliseconds since epoch."""
    dt = datetime.fromisoformat(ts_str)
    return int(dt.timestamp() * 1000)


def load_train_logs():
    """Load train log data from JSON file"""
    try:
        with open(TRAIN_LOGS_PATH, 'r') as f:
            data = json.load(f)
            logging.info(f"Loaded {len(data)} train log entries.")
            return data
    except Exception as e:
        logging.error(f"Failed to load train logs: {e}")
        return []


def load_ambulance_logs():
    """Load ambulance log data from JSON file."""
    try:
        with open(AMB_LOGS_PATH, 'r') as f:
            data = json.load(f)
            logging.info(f"Loaded {len(data)} ambulance log entries.")
            return data
    except Exception as e:
        logging.error(f"Failed to load ambulance logs: {e}")
        return []


tile38 = redis.Redis(host='tile38', port=9851, decode_responses=True)


def produce_train_messages():
    """Produce train location messages and create geofences for stopped trains"""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        api_version=(3, 0, 0),
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    try:
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
                # logging.info(f"📤 Sending train data: {message}")
                producer.send('train-locations', value=message)

                # # Create geofence if train has stopped
                # if train['snelheid'] == 0:
                #     tile38.execute_command(f"SET trains {train['ritId']} POINT {train['lat']} {train['lng']}")
                #     logging.info(f"🚧 Geofence set for stopped train {train['ritId']} at ({train['lat']}, {train['lng']})")

            producer.flush()
            time.sleep(5)

    except Exception as e:
        logging.error(f"🚨 Train producer error: {e}")
    finally:
        producer.close()


def produce_ambulance_messages():
    """Produce ambulance location messages in batches based on timestamps"""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        api_version=(3, 0, 0),
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    try:
        ambulance_logs = load_ambulance_logs()  # Load ambulance logs
        timestamp_groups = {}

        # Group ambulance data by timestamp
        for entry in ambulance_logs:
            timestamp = parse_timestamp(entry['timestamp'])
            if timestamp not in timestamp_groups:
                timestamp_groups[timestamp] = []

            for amb in entry.get('ambulances', []):
                message = {
                    "timestamp": timestamp,
                    "vehicle_number": amb.get("id"),
                    "lat": amb.get("lat"),
                    "lng": amb.get("lng"),  # Consistent with train 'lng' key
                    "speed": amb.get("speed_kmh"),
                    "heading": amb.get("heading"),
                    "accuracy": amb.get("accuracy_m"),
                    "type": amb.get("type"),
                    "source": amb.get("source")
                }
                timestamp_groups[timestamp].append(message)

        # Send messages in batches every 5 seconds
        for timestamp, messages in timestamp_groups.items():
            for message in messages:
                # logging.info(f"📤 Sending ambulance data: {message}")
                producer.send('ambulance-locations', value=message)
            producer.flush()
            # logging.info(f"✅ Ambulance data batch for timestamp {timestamp} sent.")
            time.sleep(2)  # Optional delay between batches

    except Exception as e:
        logging.error(f"🚨 Ambulance producer error: {e}")
    finally:
        producer.close()


def main():
    try:
        ensure_topics_exist()

        train_thread = threading.Thread(
            target=produce_train_messages, daemon=True)
        ambulance_thread = threading.Thread(
            target=produce_ambulance_messages, daemon=True)

        train_thread.start()
        ambulance_thread.start()

        # Keep main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Stopping producer...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        logging.info("Application shutdown complete")


if __name__ == "__main__":
    main()
