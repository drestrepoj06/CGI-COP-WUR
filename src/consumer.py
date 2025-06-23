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
    # busy_objects = []

    # try:
    #     # 判断 ambu_path2train 集合是否有内容（cursor 不为 0 表示还有数据）
    #     cursor, response = tile38.execute_command("SCAN", "ambu_path2train", "LIMIT", 1)
    #     if not response or not isinstance(response, list) or len(response) == 0:
    #         return busy_objects  # 🚫 集合为空，直接返回空列表

    #     # 重新执行带 WITHOBJECT 和 WITHFIELDS 的完整查询
    #     cursor, response = tile38.execute_command("SCAN", "ambu_path2train", "WITHOBJECT", "WITHFIELDS")
    #     if response and isinstance(response, list):
    #         for obj in response:
    #             full_id = obj[0]
    #             parts = full_id.split("_")
    #             if len(parts) >= 3:
    #                 ambulance_id = parts[-1]

    #                 geojson = json.loads(obj[1]) if isinstance(obj[1], str) else {}
    #                 coordinates = geojson.get("coordinates", [])
    #                 lng, lat = coordinates[0], coordinates[1]
    #                 timestamp = coordinates[2] if len(coordinates) >= 3 else None

    #                 fields = obj[2] if len(obj) > 2 else []
    #                 info_json = None
    #                 for field in fields:
    #                     if isinstance(field, dict) and "route_points_timed" in field:
    #                         info_json = field
    #                         break

    #                 if info_json:
    #                     busy_objects.append({
    #                         "ambulance_id": ambulance_id,
    #                         "accident_coordinates": [lat, lng],
    #                         "incident_time": timestamp,
    #                         "travel_time": info_json.get("travel_time"),
    #                         "route_points_timed": info_json.get("route_points_timed")
    #                     })

    ############## ############## ############## ##############
    ############## ############## ############## ##############

    # busy_objects = []
    # try:
    #     response = tile38.execute_command(
    #         "SEARCH", "ambu_path2train",
    #         "MATCH", "*",
    #         "LIMIT", 9999,
    #         "WITHOBJECT", "WITHFIELDS"
    #     )

    #     if not isinstance(response, dict) or "objects" not in response:
    #         return busy_objects

    #     for obj in response["objects"]:
    #         full_id = obj.get("id", "")
    #         parts = full_id.split("_")
    #         if len(parts) < 3:
    #             continue
    #         ambulance_id = parts[-1]

    #         geojson = obj.get("object", {})
    #         coordinates = geojson.get("coordinates", [])
    #         if len(coordinates) < 2:
    #             continue
    #         lng, lat = coordinates[0], coordinates[1]
    #         timestamp = coordinates[2] if len(coordinates) >= 3 else None

    #         info_json = {}
    #         for field in obj.get("fields", []):
    #             if isinstance(field, dict) and "info" in field:
    #                 try:
    #                     info_json = json.loads(field["info"])
    #                 except Exception as e:
    #                     logging.warning(f"Failed to parse 'info' field: {e}")
    #                 break

    #         if info_json.get("route_points_timed"):
    #             busy_objects.append({
    #                 "ambulance_id": ambulance_id,
    #                 "accident_coordinates": [lat, lng],
    #                 "incident_time": timestamp,
    #                 "travel_time": info_json.get("travel_time"),
    #                 "route_points_timed": info_json["route_points_timed"]
    #             })

    ############## ############## ############## ##############
    ############## ############## ############## ##############

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
                    fields["is_within_area"] = is_within_visible_radius(
                        lat, lng)  # New visibility flag
                    fields["accident_location"] = None

                elif topic == "ambulance-locations":  # ambulance-locations
                    # 提前获取已有的救护车路径对象关联的 vehicle_number 列表
                    # 提前获取已分配任务的救护车 ID 列表（如有）
                    ambu_related_objects = get_busy_object_ids_from_ambu_path()
                    ambu_related_ids = [
                        obj["ambulance_id"] for obj in ambu_related_objects] if ambu_related_objects else []

                    # 当前消息中的救护车 ID 和对象 ID
                    vehicle_number = msg.get("vehicle_number")
                    object_id = vehicle_number if vehicle_number else f"ambulance_{int(timestamp_ms)}"

                    # logging.info("ambu object_id: ")
                    # logging.info(object_id)
                    # logging.info("ambu_related_ids: ")
                    # logging.info(ambu_related_ids)

                    # Step 1: 先构建常规字段
                    fields = {
                        "vehicle_number": vehicle_number,
                        "speed": msg.get("speed"),
                        "heading": msg.get("heading"),
                        "accuracy": msg.get("accuracy"),
                        "type": msg.get("type"),
                        "source": msg.get("source"),
                        "accident_location": None,  # 可以在未来动态更新
                        "availability_status": msg.get("availability_status", True)
                    }

                    # Step 2: 判断是否“冻结”（已分配救援任务）
                    if vehicle_number in ambu_related_ids:
                        # logging.info("vehicle_numer already in ambu_related_ids")
                        fields["status"] = False  # 冻结
                    else:
                        fields["status"] = True

                    # logging.info("ambu status: ")
                    # logging.info(fields["status"])

                # Initialize existing_fields and update logic
                existing_fields = {}
                skip_update = False

                try:
                    existing = tile38.execute_command(
                        "GET", collection, object_id, "WITHFIELDS", "OBJECT")
                    if existing and isinstance(existing, list) and len(existing) >= 2:
                        existing_obj = json.loads(existing[0])
                        existing_fields = json.loads(
                            existing[1][1])  # ['info', '{...}']

                        # Check if the object is frozen
                        if existing_fields.get("status") is False:
                            skip_update = True
                            # logging.info(f"🚫 Skipping update for frozen {collection} {object_id}")
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
                            logging.info(
                                f"update ambu-{object_id} <- matched-coords {matched_coord[0]} {matched_coord[1]} since false status {matched_coord[2]}")
                        else:
                            logging.warning(
                                f"⚠️ No matched coordinates found for {vehicle_number}, skipping POINT update.")
                    else:
                        logging.info(
                            f"update ambu-{object_id} <- consumer {lat} {lng} since not false status {timestamp_ms}")
                        tile38.execute_command(
                            "SET", collection, object_id,
                            "FIELD", "info", json.dumps(fields),
                            "POINT", lat, lng, timestamp_ms
                        )

                latest_broken = None
                latest_timestamp = -1

                ##############

                try:
                    cursor, response = tile38.execute_command(
                        "SCAN", "broken_train")

                    if response and isinstance(response, list):
                        for obj in response:
                            try:

                                object_id = obj[0].split("_")[1]  # train_id
                                # {"type":"Point","coordinates":[5.1060586,52.093464,1750411271244]}
                                geojson_obj = json.loads(obj[1])

                                # logging.info(f"🕵️ geojson_obj = {geojson_obj} (type: {type(geojson_obj)})")

                                # [5.1060586,52.093464,1750411271244]
                                coords = geojson_obj.get("coordinates", [])

                                timestamp = coords[2]  # 1750411271244
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
                                logging.warning(
                                    f"Failed to parse broken_train obj {obj}: {e}")

                    if latest_broken:  # only execute this after create incident
                        # logging.info(f"🚨 Latest incident: {latest_broken['object_id']} at {latest_broken['timestamp']}")

                        cursor = 0
                        existing_object_ids = []

                        while True:
                            cursor, result = tile38.execute_command(
                                "SCAN", "ambu_path2train", "MATCH", "*_ambu_*")
                            objects = result if isinstance(
                                result, list) else []

                            for obj in objects:
                                key = obj[0] if isinstance(
                                    obj, list) else obj.get("id", "")
                                if "_ambu_" in key:
                                    existing_object_id = key.split("_ambu_")[0]
                                    existing_object_ids.append(
                                        existing_object_id)

                            if int(cursor) == 0:
                                break

                        # logging.info(f"existing_object_ids: {existing_object_ids}")
                        # logging.info(f"latest_broken['object_id']: {latest_broken['object_id']}")

                        if latest_broken['object_id'] not in existing_object_ids:
                            ambulance_positions = fetch_ambulance_positions()
                            # {'id': '1', 'lat': 52.07583, 'lng': 5.03176}, {'id': '10', 'lat': 52.12133676463085, 'lng': 5.035003451829178}

                            # {"id": 1,"lat": coords[1], "lng": coords[0]}, {"id": 2,"lat": coords[1], "lng": coords[0]}

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

                            route_data = best_route.get("route")

                            if isinstance(route_data, dict):
                                travel_time = route_data["routes"][0]["legs"][0]["summary"]["travelTimeInSeconds"]
                            else:
                                logging.error(
                                    "❌ best_route['route'] is not a dict! Actual value:")
                                logging.error(route_data)

                            # logging.info(f"best_route eta time: {travel_time}")
                            # logging.info(travel_time)

                            try:

                                ambu_path_ambu_id_train_id = f"{latest_broken['object_id']}_ambu_{best_route['ambulance_id']}"

                                existing = tile38.execute_command(
                                    "GET", "ambu_path2train", ambu_path_ambu_id_train_id)

                                # "6737_ambu_1"

                                # logging.info("existing: ")
                                # logging.info(existing)

                                if not existing:
                                    route_points = best_route["route"]["routes"][0]["legs"][0]["points"]
                                    travel_time = best_route["route"]["routes"][0]["legs"][0]["summary"]["travelTimeInSeconds"]
                                    start_timestamp = latest_broken["timestamp"]

                                    num_points = len(route_points)
                                    timed_points = []
                                    if num_points > 0:
                                        interval = travel_time / num_points
                                        # logging.info("interval: ")
                                        # logging.info(interval)

                                        for idx, pt in enumerate(route_points):
                                            # 转为毫秒级时间戳
                                            eta = int(start_timestamp +
                                                      (idx * interval * 1500))

                                            # logging.info(pt["latitude"])
                                            # logging.info(pt["longitude"])
                                            timed_points.append({
                                                "latitude": pt["latitude"],
                                                "longitude": pt["longitude"],
                                                "timestamp": eta  # or "z": eta
                                            })

                                    ambu_path_fields = {
                                        "ambulance_id": best_route["ambulance_id"],
                                        "travel_time": travel_time,
                                        # "route_points": route_points,
                                        "route_points_timed": timed_points  # ✅ 新增字段，包含时间信息
                                    }

                                    tile38.execute_command(
                                        "SET", "ambu_path2train", ambu_path_ambu_id_train_id,
                                        "FIELD", "info", json.dumps(
                                            ambu_path_fields),
                                        "OBJECT", json.dumps(
                                            latest_broken["geojson"])
                                    )

                                # else:
                                #     logging.info(f"🚫 ID {ambu_path_ambu_id_train_id} already exists in ambu_path2train. Skipped.")
                            except Exception as e:
                                logging.error(
                                    f"⚠️ Error checking for existing ambu_path2train object {ambu_path_ambu_id_train_id}: {e}")

                ##############

                except Exception as e:
                    logging.error(
                        f"🚨 Error while scanning and processing broken_train data: {e}")

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
