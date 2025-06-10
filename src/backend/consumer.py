import json
import logging
import sys
import os
import time

import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
from kafka import KafkaConsumer

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Kafka setup
KAFKA_BROKER = 'kafka:9092'
TOPICS = ['train-locations', 'ambulance-locations']
GROUP_ID = 'vehicle-location-consumers'

# Output folders
OUTPUT_DIRS = {
    'train-locations': 'train_location_data',
    'ambulance-locations': 'ambulance_location_data'
}

for folder in OUTPUT_DIRS.values():
    os.makedirs(folder, exist_ok=True)


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


def create_empty_geodf(columns):
    """Initialize an empty GeoDataFrame with geometry."""
    return gpd.GeoDataFrame(columns=columns, geometry='geometry', crs='EPSG:4326')


def message_to_georow(msg, topic):
    """Convert Kafka message to a dictionary row with geometry."""
    try:
        lat = float(msg.get("lat"))
        lng = float(msg.get("lng"))
        geometry = Point(lng, lat)

        if topic == 'train-locations':
            row = {
                "timestamp": msg.get("timestamp"),
                "type": msg.get("type"),
                "ritId": msg.get("ritId"),
                "speed": msg.get("speed"),
                "direction": msg.get("direction"),
                "geometry": geometry
            }
        else:  # ambulance-locations
            row = {
                "timestamp": msg.get("timestamp"),
                "vehicle_number": msg.get("vehicle_number"),
                "speed": msg.get("speed"),
                "heading": msg.get("heading"),
                "accuracy": msg.get("accuracy"),
                "type": msg.get("type"),
                "source": msg.get("source"),
                "geometry": geometry
            }
        return row
    except Exception as e:
        logging.error(f"🚨 Failed to parse message into GeoRow: {e}")
        return None


def main():
    consumer = create_kafka_consumer()

    # Grouping state for each topic
    current_timestamps = {
        'train-locations': None,
        'ambulance-locations': None
    }

    current_geodfs = {
        'train-locations': create_empty_geodf(['timestamp', 'type', 'ritId', 'speed', 'direction', 'geometry']),
        'ambulance-locations': create_empty_geodf(['timestamp', 'vehicle_number', 'speed', 'heading', 'accuracy', 'type', 'source', 'geometry'])
    }

    try:
        for message in consumer:
            topic = message.topic
            msg = message.value
            logging.info(f"📥 Received message from '{topic}': {msg}")

            msg_timestamp = msg.get("timestamp")
            if msg_timestamp is None:
                logging.warning(f"Message from {topic} missing timestamp; skipping.")
                continue

            # Set initial timestamp if needed
            if current_timestamps[topic] is None:
                current_timestamps[topic] = msg_timestamp

            # Check for timestamp switch (new group)
            if msg_timestamp != current_timestamps[topic]:
                logging.info(f"🕓 {topic} switching to new timestamp group: {msg_timestamp}")

                # Save GeoDataFrame to file
                out_path = os.path.join(OUTPUT_DIRS[topic], f"{topic.split('-')[0]}_{current_timestamps[topic]}.json")
                current_geodfs[topic].to_file(out_path, driver='GeoJSON')
                logging.info(f"💾 Saved {len(current_geodfs[topic])} records to {out_path}")

                # Reset for new group
                current_geodfs[topic] = create_empty_geodf(current_geodfs[topic].columns)
                current_timestamps[topic] = msg_timestamp

            # Convert message to GeoRow
            row = message_to_georow(msg, topic)
            if row:
                current_geodfs[topic] = pd.concat(
                    [current_geodfs[topic], gpd.GeoDataFrame([row], geometry='geometry')],
                    ignore_index=True
                )

            time.sleep(0.1)

    except KeyboardInterrupt:
        logging.info("🛑 Interrupt received, shutting down consumer...")
    except Exception as e:
        logging.error(f"🚨 Error processing message: {e}")
    finally:
        consumer.close()
        logging.info("🔚 Kafka consumer closed.")
        # Save any remaining records
        for topic, gdf in current_geodfs.items():
            if not gdf.empty and current_timestamps[topic]:
                out_path = os.path.join(OUTPUT_DIRS[topic], f"{topic.split('-')[0]}_{current_timestamps[topic]}.json")
                gdf.to_file(out_path, driver='GeoJSON')
                logging.info(f"💾 Saved final batch of {len(gdf)} records to {out_path}")



if __name__ == "__main__":
    main()
