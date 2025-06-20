import json
import logging
import sys
import redis
from kafka import KafkaConsumer

from utils.navigate import fetch_ambulance_positions, get_route

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


def get_busy_object_ids_from_ambu_path():
    object_ids = []
    try:
        cursor, response = tile38.execute_command("SCAN", "ambu_path2train")
        if response and isinstance(response, list):
            for obj in response:
                full_id = obj[0]  # e.g., "103944_ambu_3"
                parts = full_id.split("_")
                if len(parts) >= 3:
                    ambulance_id = parts[-1]  # 最后一部分是我们想要的数字部分
                    object_ids.append(ambulance_id)
    except Exception as e:
        logging.error(f"Failed to scan ambu_path2train: {e}")
    
    return object_ids



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
                    fields["accident_location"] = None
                    
                else:  # ambulance-locations
                    # 提前获取已有的救护车路径对象关联的 vehicle_number 列表
                    ambu_related_ids = get_busy_object_ids_from_ambu_path()

                    object_id = msg.get("vehicle_number", f"ambulance_{int(timestamp_ms)}")
                    vehicle_number = msg.get("vehicle_number")

                    fields["vehicle_number"] = vehicle_number
                    fields["speed"] = msg.get("speed")
                    fields["heading"] = msg.get("heading")
                    fields["accuracy"] = msg.get("accuracy")
                    fields["type"] = msg.get("type")
                    fields["source"] = msg.get("source")

                    # 如果这个救护车已经被分配到一条 ambu_path2train 中了，就标记为“冻结”状态
                    if vehicle_number in ambu_related_ids:
                        fields["status"] = False
                    else:
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




                # 扫描 broken_train 中的所有对象，找出时间最新的一条
                latest_broken = None
                latest_timestamp = -1

                try:
                    cursor, response = tile38.execute_command("SCAN", "broken_train")
                    if response and isinstance(response, list):
                        for obj in response:
                            try:
                                object_id = obj[0].split("_")[1] 
                                geojson_obj = json.loads(obj[1])
                                coords = geojson_obj.get("coordinates", [])

                                if len(coords) < 3:
                                    continue

                                timestamp = coords[2]
                                if timestamp > latest_timestamp:
                                    latest_timestamp = timestamp
                                    latest_broken = {
                                        "object_id": object_id,
                                        "geojson": geojson_obj,
                                        "lat": coords[1],
                                        "lng": coords[0],
                                        "timestamp": timestamp
                                    }
                            except Exception as e:
                                logging.warning(f"Failed to parse broken_train obj {obj}: {e}")

                    if latest_broken:
                        logging.info(f"🚨 Latest incident: {latest_broken['object_id']} at {latest_broken['timestamp']}")

                        ambulance_positions = fetch_ambulance_positions()

                        logging.info(ambulance_positions)
                        logging.info("ambulance_positions")

                        ambu_routes = [
                            {
                                "ambulance_id": ambu["id"],
                                "route": get_route((ambu["lat"], ambu["lng"]), (latest_broken["lat"], latest_broken["lng"]))
                            }
                            for ambu in ambulance_positions
                        ]

                        best_route = min(
                            ambu_routes,
                            key=lambda x: x["route"]["routes"][0]["legs"][0]["summary"]["travelTimeInSeconds"]
                        )
                        
                        # logging.info(best_route)
                        logging.info("best_route eta time: ")
                        logging.info({best_route["route"]["routes"][0]["legs"][0]["summary"]["travelTimeInSeconds"]})

                        ambu_path_fields = {
                            "ambulance_id": best_route["ambulance_id"],
                            "travel_time": best_route["route"]["routes"][0]["legs"][0]["summary"]["travelTimeInSeconds"],
                            "route_points": best_route["route"]["routes"][0]["legs"][0]["points"]
                        }

                        ambu_path_ambu_id_train_id = f"{latest_broken['object_id']}_ambu_{best_route["ambulance_id"]}"

                        
                        tile38.execute_command(
                            "SET", "ambu_path2train", ambu_path_ambu_id_train_id,
                            "FIELD", "info", json.dumps(ambu_path_fields),
                            "OBJECT", json.dumps(latest_broken["geojson"])
                        )

                except Exception as e:
                    logging.error(f"🚨 Error while scanning and processing broken_train data: {e}")






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
