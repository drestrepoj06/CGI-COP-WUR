import json
import logging
import sys
import os
import time

import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
TOPICS = ['train-locations', 'ambulance-locations']  # 两个 topic

# Directories to store GeoJSON files
OUTPUT_FOLDERS = {
    "train-locations": "data/train_location_data",
    "ambulance-locations": "data/ambulance_location_data"
}

def create_kafka_consumer():
    """
    Create a Kafka consumer instance.
    This consumer will listen to multiple topics,
    deserialize incoming messages from JSON, and start reading from the earliest offset.
    """
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',  # 从最早的消息开始读取
            enable_auto_commit=True,
            group_id='location-consumers',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"✅ Connected to Kafka Broker {KAFKA_BROKER}, subscribed to topics: {', '.join(TOPICS)}")
        return consumer
    except Exception as e:
        logging.error(f"❌ Failed to create Kafka consumer: {e}")
        sys.exit(1)

def create_empty_geodf():
    """
    Create an empty GeoDataFrame with predefined columns.
    The geometry column stores Point objects.
    """
    columns = ['timestamp', 'type', 'ritId', 'speed', 'direction', 'geometry']
    gdf = gpd.GeoDataFrame(columns=columns, geometry='geometry')
    return gdf

def update_geodf(geodf, message):
    """
    Update the GeoDataFrame with data from a single message.
    """
    try:
        lat = float(message.get("lat"))
        lng = float(message.get("lng"))
        geometry = Point(lng, lat)

        row = {
            "timestamp": message.get("timestamp"),
            "type": message.get("type"),
            "ritId": message.get("ritId"),
            "speed": message.get("speed"),
            "direction": message.get("direction"),
            "geometry": geometry
        }
        
        new_row_df = gpd.GeoDataFrame([row], columns=geodf.columns, geometry="geometry")
        new_row_df.set_crs(epsg=4326, inplace=True)

        if geodf.crs is None:
            geodf.set_crs(epsg=4326, inplace=True)

        updated_gdf = pd.concat([geodf, new_row_df], ignore_index=True)
        return updated_gdf
    except Exception as e:
        logging.error(f"Error updating GeoDataFrame: {e}")
        return geodf

def write_geodf_to_file(geodf, timestamp, topic):
    """
    Write the GeoDataFrame to a GeoJSON file in the appropriate directory.
    """
    try:
        output_folder = OUTPUT_FOLDERS.get(topic, "data/unknown_location_data")
        os.makedirs(output_folder, exist_ok=True)
        file_path = os.path.join(output_folder, f"{timestamp}.geojson")

        geodf.to_file(file_path, driver="GeoJSON")
        logging.info(f"✅ Data saved for {topic} at '{file_path}'")
    except Exception as e:
        logging.error(f"Error writing GeoDataFrame for {topic}, timestamp {timestamp}: {e}")

def main():
    consumer = create_kafka_consumer()
    geo_dfs = {topic: create_empty_geodf() for topic in TOPICS}
    current_timestamps = {topic: None for topic in TOPICS}

    try:
        for message in consumer:
            topic = message.topic
            msg = message.value
            logging.info(f"📥 Received message from '{topic}': {msg}")

            msg_timestamp = msg.get("timestamp")
            if msg_timestamp is None:
                logging.warning(f"⚠️ Message from {topic} missing timestamp; skipping.")
                continue
            
            if current_timestamps[topic] is None:
                current_timestamps[topic] = msg_timestamp

            if msg_timestamp != current_timestamps[topic]:
                write_geodf_to_file(geo_dfs[topic], current_timestamps[topic], topic)
                geo_dfs[topic] = create_empty_geodf()
                current_timestamps[topic] = msg_timestamp

            geo_dfs[topic] = update_geodf(geo_dfs[topic], msg)
            time.sleep(0.1)

    except KeyboardInterrupt:
        logging.info("⏳ Interrupt received, shutting down consumer...")
    except Exception as e:
        logging.error(f"🚨 Error processing message: {e}")
    finally:
        consumer.close()
        logging.info("Consumer shutdown complete.")
        for topic in TOPICS:
            if current_timestamps[topic] is not None and len(geo_dfs[topic]) > 0:
                write_geodf_to_file(geo_dfs[topic], current_timestamps[topic], topic)

if __name__ == "__main__":
    main()
