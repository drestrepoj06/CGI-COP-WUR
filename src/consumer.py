import json
import logging
import sys
import redis
from kafka import KafkaConsumer
from geopy.distance import geodesic

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

RAIL_SEGMENTS_JSON_FILE_PATH = "utils/UtrechtRailsSegments.geojson"


def store_rail_segments():
    """Load JSON file and store rail segments into Tile38 as Polygon GeoJSON."""
    try:
        with open(RAIL_SEGMENTS_JSON_FILE_PATH, "r") as f:
            data = json.load(f)

        if data["type"] != "FeatureCollection":
            logging.error("Invalid JSON format: Expected FeatureCollection.")
            return
        
        for feature in data["features"]:
            object_id = feature["id"]
            geojson_data = json.dumps(feature["geometry"])  # 直接存储整个 Polygon GeoJSON
            properties = feature["properties"]

            fields = {
                "OBJECTID": properties.get("OBJECTID", object_id),
                "SHAPE_Length": properties.get("SHAPE_Length", 0),
                "SHAPE_Area": properties.get("SHAPE_Area", 0),
                "status": True  # 额外字段
            }

            # 存储到 Tile38
            tile38.execute_command(
                "SET", "railsegment", f"segment_{object_id}", "FIELD", "info", json.dumps(fields), "OBJECT", geojson_data
            )

    except Exception as e:
        logging.error(f"❌ Failed to process rail segments: {e}")



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

# Train visibility settings
CENTER = (52.094040, 5.093102)  # Same as in train_API.py
MAX_VISIBLE_DIST_KM = 6.15  # Trains beyond this are hidden

def is_within_visible_radius(lat, lng):
    """Returns True if train is within 6.15 km, False otherwise."""
    if lat is None or lng is None:
        return False
    return geodesic(CENTER, (lat, lng)).km <= MAX_VISIBLE_DIST_KM

def main():
    store_rail_segments()

    consumer = create_kafka_consumer()

    try:
        for message in consumer:
            topic = message.topic
            msg = message.value

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
                    fields["status"] = True
                    fields["is_within_area"] = is_within_visible_radius(lat, lng)  # New visibility flag
                    fields["accident_location"] = None
                    
                else:  # ambulance-locations
                    object_id = msg.get("vehicle_number", f"ambulance_{int(timestamp_ms)}")
                    fields["vehicle_number"] = msg.get("vehicle_number")
                    fields["speed"] = msg.get("speed")
                    fields["heading"] = msg.get("heading")
                    fields["accuracy"] = msg.get("accuracy")
                    fields["type"] = msg.get("type")
                    fields["source"] = msg.get("source")
                    fields["status"] = True
                    fields["accident_location"] = None

                # Initialize existing_fields and update logic
                existing_fields = {}
                skip_update = False

                try:
                    existing = tile38.execute_command("GET", collection, object_id, "WITHFIELDS", "OBJECT")
                    if existing and isinstance(existing, list) and len(existing) >= 2:
                        existing_obj = json.loads(existing[0])
                        existing_fields = json.loads(existing[1][1])  # ['info', '{...}']

                        # Check if the object is frozen
                        if existing_fields.get("status") is False:
                            skip_update = True
                            logging.info(f"🚫 Skipping update for frozen {collection} {object_id}")
                        elif "frozen_coords" in existing_fields:
                            fields["frozen_coords"] = existing_fields["frozen_coords"]

                except Exception as e:
                    logging.warning(f"⚠️ Could not fetch existing data for {collection}/{object_id}: {e}")

                # Decide how to store the object in Tile38
                if not skip_update:
                    if fields.get("status") is False:
                        fields["frozen_coords"] = [lng, lat, timestamp_ms]
                        geometry = json.dumps({
                            "type": "Point",
                            "coordinates": fields["frozen_coords"]
                        })
                        tile38.execute_command("SET", collection, object_id, "FIELD", "info", json.dumps(fields), "OBJECT", geometry)
                    else:
                        tile38.execute_command("SET", collection, object_id, "FIELD", "info", json.dumps(fields), "POINT", lat, lng, timestamp_ms)

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
