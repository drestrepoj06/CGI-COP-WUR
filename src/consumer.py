import json
import logging
import sys
import redis
from shapely.geometry import Point
from kafka import KafkaConsumer

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Kafka setup
KAFKA_BROKER = 'kafka:9092'
TOPICS = ['train-locations', 'ambulance-locations']
GROUP_ID = 'vehicle-location-consumers'

# Tile38 setup
TILE38_HOST = 'tile38'
TILE38_PORT = 9851
tile38 = redis.Redis(host=TILE38_HOST, port=TILE38_PORT, decode_responses=True)


def create_kafka_consumer():
    """Create KafkaConsumer subscribed to both train and ambulance topics."""
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"✅ Connected to Kafka and subscribed to topics: {TOPICS}")
        return consumer
    except Exception as e:
        logging.error(f"❌ KafkaConsumer creation failed: {e}")
        sys.exit(1)


def main():
    consumer = create_kafka_consumer()

    try:
        for message in consumer:
            topic = message.topic
            msg = message.value
            # logging.info(f"📥 Received message from '{topic}': {msg}")

            collection = topic.split('-')[0]  # 'train' or 'ambulance'

            try:
                lat = float(msg["lat"])
                lng = float(msg["lng"])
                timestamp_ms = float(msg["timestamp"])  # e.g., 1749632389587
                
                fields = {}

                if topic == "train-locations":
                    object_id = msg.get("ritId", f"train_{int(timestamp_ms)}")
                    fields["type"] = msg.get("type")
                    fields["speed"] = msg.get("speed")
                    fields["direction"] = msg.get("direction")
                else:  # ambulance-locations
                    object_id = msg.get("vehicle_number", f"ambulance_{int(timestamp_ms)}")
                    fields["vehicle_number"] = msg.get("vehicle_number")
                    fields["speed"] = msg.get("speed")
                    fields["heading"] = msg.get("heading")
                    fields["accuracy"] = msg.get("accuracy")
                    fields["type"] = msg.get("type")
                    fields["source"] = msg.get("source")

                # logging.info(json.dumps(fields))
                tile38.execute_command("SET", collection, object_id, "FIELD", "info", json.dumps(fields), "POINT", lat, lng, timestamp_ms)
                # logging.info(f"📡 Sent {collection} object {object_id} with fields {fields} and Z={timestamp_ms} to Tile38.")

            except Exception as e:
                logging.error(f"❌ Failed to send point to Tile38: {e}")

    except KeyboardInterrupt:
        logging.info("🛑 Interrupt received, shutting down consumer...")
    except Exception as e:
        logging.error(f"🚨 Error processing message: {e}")
    finally:
        consumer.close()
        logging.info("🔚 Kafka consumer closed.")


if __name__ == "__main__":
    main()
