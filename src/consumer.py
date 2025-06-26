import json
import logging
import sys
import redis
from kafka import KafkaConsumer
from geopy.distance import geodesic

from utils.navigate import fetch_ambulance_positions, get_route


# Logging config
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# Kafka setup
KAFKA_BROKER = 'kafka:9092'
TOPICS = ['train-locations', 'ambulance-locations']
GROUP_ID = 'vehicle-location-consumers'

# Tile38 setup
TILE38_HOST = 'tile38'
TILE38_PORT = 9851
tile38 = redis.Redis(host=TILE38_HOST, port=TILE38_PORT, decode_responses=True)

RAIL_SEGMENTS_JSON_FILE_PATH = "utils/UtrechtRailsSegments.geojson"


def process_broken_trains_and_assign_ambulances():
    dispatched_ambulances = []  # 记录成功派遣的救护车信息
    latest_broken = None
    latest_timestamp = -1

    try:
        cursor, response = tile38.execute_command("SCAN", "broken_train")
        if response and isinstance(response, list):
            for obj in response:
                try:
                    # 验证 obj 的结构
                    if not (isinstance(obj, list) and len(obj) >= 3):
                        logging.warning(f"Unexpected object structure: {obj}, skipping.")
                        continue

                    # 提取事件 ID
                    event_id = obj[0]
                    object_id = event_id.split("_")[1]

                    # 提取 GeoJSON 对象
                    geojson_str = obj[1]
                    try:
                        geojson_obj = json.loads(geojson_str)
                    except json.JSONDecodeError as e:
                        logging.warning(f"Failed to parse GeoJSON for {event_id}: {e}")
                        continue

                    # 提取字段列表
                    fields_list = obj[2]
                    if not isinstance(fields_list, list) or len(fields_list) % 2 != 0:
                        logging.warning(f"Invalid fields format for {event_id}: {fields_list}, skipping.")
                        continue

                    # 将字段列表转换为字典
                    fields_dict = dict(zip(fields_list[::2], fields_list[1::2]))

                    # 提取 ambulance_units，默认为 1
                    try:
                        ambulance_count = int(fields_dict.get("ambulance_units", "1"))
                    except ValueError:
                        logging.warning(f"Invalid ambulance_units value for {event_id}, defaulting to 1.")
                        ambulance_count = 1

                    # 提取地理信息和时间戳
                    coords = geojson_obj.get("coordinates", [])
                    timestamp = int(coords[2]) if len(coords) > 2 else -1

                    # 更新最新事件
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
                    cursor = 0
                    existing_object_ids = []
                    while True:
                        cursor, result = tile38.execute_command(
                            "SCAN", "ambu_path2train", "MATCH", "*_ambu_*")
                        objects = result if isinstance(result, list) else []
                        for obj in objects:
                            key = obj[0] if isinstance(
                                obj, list) else obj.get("id", "")
                            if "_ambu_" in key:
                                existing_object_id = key.split("_ambu_")[0]
                                existing_object_ids.append(existing_object_id)
                        if int(cursor) == 0:
                            break

                    if latest_broken['object_id'] not in existing_object_ids:
                        spare_ambu_positions, busy_ambu_positions = fetch_ambulance_positions()

                        ambu_routes = [
                            {
                                "ambulance_id": ambu["id"],
                                "route": get_route((ambu["lat"], ambu["lng"]), (latest_broken["lat"], latest_broken["lng"]))
                            }
                            for ambu in spare_ambu_positions
                        ]

                        for route_info in ambu_routes:
                            logging.debug(f"[route check] ambulance {route_info['ambulance_id']} route: {route_info['route']}")

                        ambu_routes_sorted = sorted(
                            ambu_routes,
                            key=lambda x: x["route"]["routes"][0]["legs"][0]["summary"]["travelTimeInSeconds"]
                        )
                        selected_routes = ambu_routes_sorted[:ambulance_count]

                        for route_info in selected_routes:
                            ambulance_id = route_info["ambulance_id"]
                            route_data = route_info["route"]
                            if not isinstance(route_data, dict):
                                logging.error(
                                    f"Route data is not dict for ambulance {ambulance_id}")
                                continue
                            leg = route_data["routes"][0]["legs"][0]
                            route_points = leg["points"]
                            travel_time = leg["summary"]["travelTimeInSeconds"]
                            start_timestamp = latest_broken["timestamp"]
                            timed_points = build_timed_route_points(
                                route_points, start_timestamp, travel_time)
                            ambu_path_fields = {
                                "ambulance_id": ambulance_id,
                                "travel_time": travel_time,
                                "route_points_timed": timed_points
                            }
                            ambu_path_key = f"{latest_broken['object_id']}_ambu_{ambulance_id}"
                            existing = tile38.execute_command(
                                "GET", "ambu_path2train", ambu_path_key)
                            if not existing:
                                tile38.execute_command(
                                    "SET", "ambu_path2train", ambu_path_key,
                                    "FIELD", "info", json.dumps(ambu_path_fields),
                                    "OBJECT", json.dumps(latest_broken["geojson"])
                                )
                                dispatched_ambulances.append({
                                    "ambulance_id": ambulance_id,
                                    "train_id": latest_broken["object_id"],
                                    "travel_time": travel_time
                                })

    except Exception as e:
        logging.error(
            f"🚨 Error while scanning and processing broken_train data: {e}")
        return {"status": "error", "error": str(e), "dispatched": []}

    return {
        "status": "success",
        "train_id": latest_broken["object_id"] if latest_broken else None,
        "dispatched": dispatched_ambulances
    }


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
            # Polygon GeoJSON
            geojson_data = json.dumps(feature["geometry"])
            properties = feature["properties"]

            fields = {
                "OBJECTID": properties.get("OBJECTID", object_id),
                "SHAPE_Length": properties.get("SHAPE_Length", 0),
                "SHAPE_Area": properties.get("SHAPE_Area", 0),
                "status": True
            }

            # 存储到 Tile38
            tile38.execute_command(
                "SET", "railsegment", f"segment_{object_id}", "FIELD", "info", json.dumps(
                    fields), "OBJECT", geojson_data
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
        logging.info(
            f"✅ Connected to Kafka and subscribed to topics: {TOPICS}")
        return consumer
    except Exception as e:
        logging.error(f"❌ KafkaConsumer creation failed: {e}")
        sys.exit(1)


def get_busy_object_ids_from_ambu_path():

    busy_objects = []
    try:
        cursor = 0
        while True:
            cursor, response = tile38.execute_command(
                "SCAN", "ambu_path2train", "CURSOR", cursor, "LIMIT", 1000)
            if not response or not isinstance(response, list):
                break

            for obj in response:
                full_id = obj[0]  # e.g., "incident_3144_ambu_3"
                parts = full_id.split("_")
                if len(parts) >= 3:
                    ambulance_id = parts[-1]

                    # 解析 GeoJSON 对象
                    geojson_str = obj[1]
                    try:
                        geojson = json.loads(geojson_str)
                        coordinates = geojson.get("coordinates", [])
                        if len(coordinates) < 2:
                            continue
                        lng, lat = coordinates[0], coordinates[1]
                        timestamp = coordinates[2] if len(
                            coordinates) >= 3 else None
                    except Exception as e:
                        logging.warning(
                            f"Invalid GeoJSON in object {full_id}: {e}")
                        continue

                    # 解析字段，通常 obj[2] 是 ['info', '{"travel_time":..., "route_points_timed": [...]}']
                    info_json = {}
                    if len(obj) > 2:
                        fields = obj[2]
                        for i in range(0, len(fields), 2):
                            if fields[i] == "info":
                                try:
                                    info_json = json.loads(fields[i + 1])
                                except Exception as e:
                                    logging.warning(
                                        f"Failed to parse field info for {full_id}: {e}")
                                break

                    if info_json.get("route_points_timed"):
                        busy_objects.append({
                            "ambulance_id": ambulance_id,
                            "accident_coordinates": [lat, lng],
                            "incident_time": timestamp,
                            "travel_time": info_json.get("travel_time"),
                            "route_points_timed": info_json["route_points_timed"]
                        })

            if str(cursor) == "0":
                break

    except Exception as e:
        logging.error(f"Failed to scan ambu_path2train: {e}")

    return busy_objects


# Train visibility settings
CENTER = (52.094040, 5.093102)  # Same as in train_API.py
MAX_VISIBLE_DIST_KM = 6.15  # Trains beyond this are hidden


def is_within_visible_radius(lat, lng):
    """Returns True if train is within 6.15 km, False otherwise."""
    if lat is None or lng is None:
        return False
    return geodesic(CENTER, (lat, lng)).km <= MAX_VISIBLE_DIST_KM


def build_timed_route_points(route_points, start_timestamp, travel_time):
    """
    构建含时间戳的路径点序列
    """
    if not route_points:
        return []

    interval = travel_time / len(route_points)
    timed_points = [
        {
            "latitude": pt["latitude"],
            "longitude": pt["longitude"],
            # 1s == 1,000 ms but we want 1s == 800 ms
            "timestamp": int(start_timestamp + idx * interval * 800)
        }
        for idx, pt in enumerate(route_points)
    ]
    return timed_points


def main():
    store_rail_segments()


    try:
        consumer = create_kafka_consumer()
        for message in consumer:
            topic = message.topic
            msg = message.value
            collection = topic.split('-')[0]  # 'train' or 'ambulance'
            try:
                lat = float(msg["lat"])
                lng = float(msg["lng"])
                timestamp_ms = float(msg["timestamp"])
                fields = {}
                if topic == "train-locations":
                    object_id = msg.get("ritId", f"train_{int(timestamp_ms)}")
                    fields["type"] = msg.get("type")
                    fields["speed"] = msg.get("speed")
                    fields["direction"] = msg.get("direction")
                    fields["status"] = True
                    fields["is_within_area"] = is_within_visible_radius(
                        lat, lng)
                    fields["accident_location"] = None
                elif topic == "ambulance-locations":
                    ambu_related_objects = get_busy_object_ids_from_ambu_path()
                    ambu_related_ids = [obj["ambulance_id"]
                                        for obj in ambu_related_objects] if ambu_related_objects else []
                    vehicle_number = msg.get("vehicle_number")
                    object_id = vehicle_number if vehicle_number else f"ambulance_{int(timestamp_ms)}"
                    fields = {
                        "vehicle_number": vehicle_number,
                        "speed": msg.get("speed"),
                        "heading": msg.get("heading"),
                        "accuracy": msg.get("accuracy"),
                        "type": msg.get("type"),
                        "source": msg.get("source"),
                        "accident_location": None,
                        "station_name": msg.get("station_name"),
                        "availability_status": msg.get("availability_status", True)
                    }
                    if vehicle_number in ambu_related_ids:
                        fields["status"] = False
                    else:
                        fields["status"] = True

                existing_fields = {}
                skip_update = False
                try:
                    existing = tile38.execute_command(
                        "GET", collection, object_id, "WITHFIELDS", "OBJECT")
                    if existing and isinstance(existing, list) and len(existing) >= 2:
                        existing_obj = json.loads(existing[0])
                        existing_fields = json.loads(existing[1][1])
                        if existing_fields.get("status") is False:
                            skip_update = True
                        elif "frozen_coords" in existing_fields:
                            fields["frozen_coords"] = existing_fields["frozen_coords"]
                except Exception as e:
                    logging.warning(
                        f"⚠️ Could not fetch existing data for {collection}/{object_id}: {e}")

                if collection == "train":
                    if not skip_update:
                        if fields.get("status") is False:
                            fields["frozen_coords"] = [lng, lat, timestamp_ms]
                            geometry = json.dumps({
                                "type": "Point",
                                "coordinates": fields["frozen_coords"]
                            })
                            tile38.execute_command("SET", collection, object_id, "FIELD", "info", json.dumps(
                                fields), "OBJECT", geometry)
                        else:
                            tile38.execute_command("SET", collection, object_id, "FIELD", "info", json.dumps(
                                fields), "POINT", lat, lng, timestamp_ms)

                elif collection == "ambulance":
                    if fields.get("status") is False:
                        matched_coord = None
                        min_time_diff = float("inf")
                        for obj in ambu_related_objects:
                            if obj["ambulance_id"] == vehicle_number:
                                for pt in obj.get("route_points_timed", []):
                                    t_diff = abs(
                                        pt["timestamp"] - timestamp_ms)
                                    if t_diff < min_time_diff:
                                        min_time_diff = t_diff
                                        matched_coord = [
                                            pt["latitude"], pt["longitude"], pt["timestamp"]]
                        if matched_coord:
                            tile38.execute_command(
                                "SET", collection, object_id,
                                "FIELD", "info", json.dumps(fields),
                                "POINT", matched_coord[0], matched_coord[1], matched_coord[2]
                            )
                            # logging.info(
                            #     f"update ambu-{object_id} <- matched-coords {matched_coord[0]} {matched_coord[1]} since false status {matched_coord[2]}")
                        else:
                            logging.warning(
                                f"⚠️ No matched coordinates found for {vehicle_number}, skipping POINT update.")
                    else:
                        tile38.execute_command(
                            "SET", collection, object_id,
                            "FIELD", "info", json.dumps(fields),
                            "POINT", lat, lng, timestamp_ms
                        )
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
