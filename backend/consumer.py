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
TOPIC = 'train-locations'

# Directory to store GeoJSON files
OUTPUT_FOLDER = "train_location_data"


def create_kafka_consumer():
    """
    Create a Kafka consumer instance.
    This consumer will listen to the 'train-locations' topic,
    deserialize incoming messages from JSON, and start reading from the earliest offset.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',  # Start reading at the earliest message
            enable_auto_commit=True,
            group_id='train-location-consumers',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"✅ Successfully connected to Kafka Broker: {KAFKA_BROKER} and subscribed to topic: {TOPIC}")
        return consumer
    except Exception as e:
        logging.error(f"❌ Failed to create Kafka consumer: {e}")
        sys.exit(1)


def create_empty_geodf():
    """
    Create an empty GeoDataFrame with the predefined columns.
    The geometry column will store Point objects.
    """
    columns = ['timestamp', 'type', 'ritId', 'speed', 'direction', 'geometry']
    # Create an empty GeoDataFrame with a column for geometry
    gdf = gpd.GeoDataFrame(columns=columns, geometry='geometry')
    return gdf


# def update_geodf(geodf, message):
#     """
#     Update the GeoDataFrame with data from a single message.
#     Expects the message dictionary to contain 'lat' and 'lng' values along with other keys.
#     """
#     try:
#         # Extract latitude and longitude; convert them to floats
#         lat = float(message.get("lat"))
#         lng = float(message.get("lng"))
#         # Create a shapely Point; note: Point expects (longitude, latitude)
#         geometry = Point(lng, lat)
#         # Create a dictionary row using the message data
#         row = {
#             "timestamp": message.get("timestamp"),  # Timestamp can be in any format (e.g., milliseconds)
#             "type": message.get("type"),
#             "ritId": message.get("ritId"),
#             "speed": message.get("speed"),
#             "direction": message.get("direction"),
#             "geometry": geometry
#         }
#         # Create a new GeoDataFrame for the row
#         new_row_df = gpd.GeoDataFrame([row], columns=geodf.columns, geometry="geometry")
#         # Use pd.concat to concatenate the new row with the existing GeoDataFrame
#         updated_gdf = pd.concat([geodf, new_row_df], ignore_index=True)
#         return updated_gdf
#     except Exception as e:
#         logging.error(f"Error updating GeoDataFrame: {e}")
#         return geodf

def update_geodf(geodf, message):
    """
    Update the GeoDataFrame with data from a single message.
    Expects the message dictionary to contain 'lat' and 'lng' values along with other keys.
    The resulting GeoDataFrame will have its CRS set to WGS84 (EPSG:4326).
    """
    try:
        # Extract latitude and longitude; convert them to floats
        lat = float(message.get("lat"))
        lng = float(message.get("lng"))
        # Create a shapely Point; note: Point expects (longitude, latitude)
        geometry = Point(lng, lat)
        # Create a dictionary row using the message data
        row = {
            "timestamp": message.get("timestamp"),  # Timestamp can be in any format (e.g., milliseconds)
            "type": message.get("type"),
            "ritId": message.get("ritId"),
            "speed": message.get("speed"),
            "direction": message.get("direction"),
            "geometry": geometry
        }
        # Create a new GeoDataFrame for the row
        new_row_df = gpd.GeoDataFrame([row], columns=geodf.columns, geometry="geometry")
        # Set the CRS for the new row GeoDataFrame to WGS84 (EPSG:4326)
        new_row_df.set_crs(epsg=4326, inplace=True)
        
        # Check and set the CRS for the main GeoDataFrame if not set
        if geodf.crs is None:
            geodf.set_crs(epsg=4326, inplace=True)
        
        # Use pd.concat to concatenate the new row with the existing GeoDataFrame
        updated_gdf = pd.concat([geodf, new_row_df], ignore_index=True)
        return updated_gdf
    except Exception as e:
        logging.error(f"Error updating GeoDataFrame: {e}")
        return geodf


def write_geodf_to_file(geodf, timestamp):
    """
    Write the GeoDataFrame to a GeoJSON file.
    The file will be saved in the OUTPUT_FOLDER directory with the filename as the given timestamp.
    """
    try:
        # Ensure the output directory exists
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        # Build the file path
        file_path = os.path.join(OUTPUT_FOLDER, f"{timestamp}.json")
        # Write the GeoDataFrame to a GeoJSON file
        geodf.to_file(file_path, driver="GeoJSON")
        logging.info(f"GeoDataFrame for timestamp {timestamp} saved to '{file_path}'")
    except Exception as e:
        logging.error(f"Error writing GeoDataFrame to file for timestamp {timestamp}: {e}")


def main():
    consumer = create_kafka_consumer()
    
    # Variables to manage grouping by timestamp
    current_timestamp = None
    current_geodf = create_empty_geodf()
    
    try:
        # Consume messages indefinitely
        for message in consumer:
            # Deserialize message (value is a dict because of the deserializer)
            msg = message.value
            logging.info(f"📥 Received message from '{TOPIC}': {msg}")
            
            # Get the timestamp from the message
            msg_timestamp = msg.get("timestamp")
            if msg_timestamp is None:
                logging.warning("Message does not contain a timestamp; skipping.")
                continue
            
            # Check if the timestamp is new (i.e., different from the current group)
            if current_timestamp is None:
                current_timestamp = msg_timestamp
            
            # logging.info(f"Current timestamp: {current_timestamp}")
            # logging.info(f"Message timestamp: {msg_timestamp}")

            if msg_timestamp != current_timestamp:
                logging.info(f"Switching to new timestamp: {msg_timestamp}")
                # Write the current group's data to file before switching timestamp groups
                write_geodf_to_file(current_geodf, current_timestamp)
                # logging.info(f"Writing current group to file: {current_timestamp}")
                # logging.info(f"Current group (timestamp {current_timestamp}) now has {len(current_geodf)} records")
                # Reset the GeoDataFrame and set the new current timestamp
                current_geodf = create_empty_geodf()
                current_timestamp = msg_timestamp
            
            # Update the current GeoDataFrame with the new message
            current_geodf = update_geodf(current_geodf, msg)
            
            # current_geodf should be the train_location_data file for frontend 
            # updates every 5 seconds


            # Log the number of records in the current GeoDataFrame
            # logging.info(f"Current group (timestamp {current_timestamp}) now has {len(current_geodf)} records")
            
            # Optionally, you can flush data periodically; here we check every record and sleep briefly
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        logging.info("Interrupt received, shutting down consumer...")
    except Exception as e:
        logging.error(f"Error processing message: {e}")
    finally:
        consumer.close()
        logging.info("Consumer shutdown complete.")
        # Write out the last group if it exists
        if current_timestamp is not None and len(current_geodf) > 0:
            write_geodf_to_file(current_geodf, current_timestamp)


if __name__ == "__main__":
    main()
