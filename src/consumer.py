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


MAX_RETRIES = 50


def get_latest_broken_train():
    """
    Scan for broken trains and return the most recent one with required metadata.
    """
    latest_broken = None
    latest_timestamp = -1

    cursor, response = tile38.execute_command("SCAN", "broken_train")
    if response and isinstance(response, list):
        for obj in response:
            try:
                if not (isinstance(obj, list) and len(obj) >= 3):
                    logging.warning(f"Invalid object: {obj}, skipping.")
                    continue

                event_id = obj[0]
                object_id = event_id.split("_")[1]

                try:
                    geojson_obj = json.loads(obj[1])
                except json.JSONDecodeError as e:
                    logging.warning(f"Invalid GeoJSON for {event_id}: {e}")
                    continue

                fields_list = obj[2]
                if not isinstance(fields_list, list) or len(fields_list) % 2 != 0:
                    logging.warning(
                        f"Invalid fields for {event_id}, skipping.")
                    continue

                fields = dict(zip(fields_list[::2], fields_list[1::2]))

                try:
                    ambulance_count = int(fields.get("ambulance_units", "1"))
                except ValueError:
                    ambulance_count = 1

                coords = geojson_obj.get("coordinates", [])
                timestamp = int(coords[2]) if len(coords) > 2 else -1

                if timestamp > latest_timestamp:
                    latest_broken = {
                        "object_id": object_id,
                        "geojson": geojson_obj,
                        "lat": coords[1],
                        "lng": coords[0],
                        "timestamp": timestamp,
                        "ambulance_count": ambulance_count
                    }
                    latest_timestamp = timestamp

            except Exception as e:
                logging.warning(f"Failed to parse train object {obj}: {e}")

    return latest_broken


def get_existing_assignments():
    """
    Get all ambulance-train assignments to avoid duplicate dispatch.
    """
    cursor = 0
    assigned_ids = []
    while True:
        cursor, result = tile38.execute_command(
            "SCAN", "ambu_path2train", "MATCH", "*_ambu_*")
        objects = result if isinstance(result, list) else []
        for obj in objects:
            key = obj[0] if isinstance(obj, list) else obj.get("id", "")
            if "_ambu_" in key:
                assigned_ids.append(key.split("_ambu_")[0])
        if int(cursor) == 0:
            break
    return assigned_ids


def assign_ambulances_to_broken_train(broken_info):
    """
    Assign one or more ambulances to the given broken train.
    """
    dispatched = []
    spare_ambus, _ = fetch_ambulance_positions()

    routes = [{
        "ambulance_id": ambu["id"],
        "route": get_route((ambu["lat"], ambu["lng"]), (broken_info["lat"], broken_info["lng"]))
    } for ambu in spare_ambus]

    routes = sorted(
        routes,
        key=lambda x: x["route"]["routes"][0]["legs"][0]["summary"]["travelTimeInSeconds"]
    )

    for route_info in routes[:broken_info["ambulance_count"]]:
        ambu_id = route_info["ambulance_id"]
        route_data = route_info["route"]
        if not isinstance(route_data, dict):
            logging.error(f"Invalid route format for {ambu_id}")
            continue
        leg = route_data["routes"][0]["legs"][0]
        points = leg["points"]
        duration = leg["summary"]["travelTimeInSeconds"]
        timed_route = build_timed_route_points(
            points, broken_info["timestamp"], duration)

        path_fields = {
            "ambulance_id": ambu_id,
            "travel_time": duration,
            "route_points_timed": timed_route
        }
        ambu_key = f"{broken_info['object_id']}_ambu_{ambu_id}"
        existing = tile38.execute_command("GET", "ambu_path2train", ambu_key)
        if not existing:
            tile38.execute_command(
                "SET", "ambu_path2train", ambu_key,
                "FIELD", "info", json.dumps(path_fields),
                "OBJECT", json.dumps(broken_info["geojson"])
            )
            dispatched.append({
                "ambulance_id": ambu_id,
                "train_id": broken_info["object_id"],
                "travel_time": duration
            })

    return dispatched


def process_broken_trains_and_assign_ambulances():
    """
    Main function to process broken train data and assign ambulances, 
    with automatic retries in case of error.
    """
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            broken = get_latest_broken_train()
            if not broken:
                return {"status": "no broken train", "dispatched": []}

            existing_ids = get_existing_assignments()
            if broken["object_id"] in existing_ids:
                return {"status": "already dispatched", "train_id": broken["object_id"], "dispatched": []}

            dispatched = assign_ambulances_to_broken_train(broken)

            return {
                "status": "success",
                "train_id": broken["object_id"],
                "dispatched": dispatched
            }

        except Exception as e:
            logging.error(f"❌ Attempt {attempt + 1} failed: {e}")
            attempt += 1

    return {"status": "error", "message": "Failed after multiple attempts."}


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

            # Save in Tile38
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
