from fastapi import FastAPI, WebSocket
import json
import asyncio
import redis
import random
import logging
import datetime
import threading
from collections import deque

import os
import re
from shapely.geometry import shape, MultiPolygon, Point, mapping
from shapely.ops import unary_union


broadcast_queue = asyncio.Queue()
websocket_connections = set()
logger = logging.getLogger("uvicorn")

# Initialize FastAPI and Redis client
app = FastAPI()
client = redis.Redis(host="tile38", port=9851, decode_responses=True)

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)


logger = logging.getLogger(__name__)


def mark_random_train_as_inactive(client):
    max_retries = 30
    for attempt in range(max_retries):
        try:
            train_ids = get_trains_within_area(client)
            selected_id = random.choice(train_ids)  # 8843 randomly choose one
            train_obj = fetch_and_freeze_train(client, selected_id)

            merged_geojson, affected_segments = update_nearby_segments(
                client, train_obj)

            create_hooks(client, merged_geojson)
            reemit_entities(client, ["train", "ambulance"])

            incident = create_incident(
                client,
                selected_id,
                train_obj["object"],
                description="Train marked inactive due to incident"
            )

            return {
                "incident": incident,
                "inactive_segments": affected_segments
            }

        except Exception as e:
            logger.error(f"❌ Failed to mark train inactive: {e}", exc_info=True)
            return None


def update_nearby_segments(client, train_obj):
    # 1. Extract the train point
    lon, lat = train_obj["object"]["coordinates"][:2]
    pt = Point(lon, lat)

    # 2. Use NEARBY to get all candidate railsegment IDs within 800m radius
    resp = client.execute_command(
        "NEARBY", "railsegment", "POINT", lat, lon, 800)
    raw_ids = [item[0].decode() if isinstance(item[0], bytes) else item[0]
               for item in (resp[1] if len(resp) > 1 else [])
               if isinstance(item, list) and item]
    logger.info(f"🛤️ Found {len(raw_ids)} nearby railsegment IDs")

    # 3. Fetch all geometries and cache them into a list
    segments = []
    for rail_id in raw_ids:
        resp = client.execute_command(
            "GET", "railsegment", rail_id, "WITHFIELDS", "OBJECT")
        if not resp or len(resp) < 2:
            continue
        geom = json.loads(resp[0])
        fields = json.loads(resp[1][1]) if len(resp[1]) >= 2 else {}
        segments.append({
            "id": rail_id,
            "geometry": geom,
            "fields": fields
        })

    if not segments:
        return []

    # 4. Merge all segment geometries
    shapes = [shape(seg["geometry"]) for seg in segments]
    merged = unary_union(shapes)

    # 5. From the merged geometry, find the polygon that contains the train point
    if merged.geom_type == "MultiPolygon":
        # Find the first polygon that contains the train point
        polys = [poly for poly in merged.geoms if poly.contains(pt)]
        if not polys:
            raise ValueError(
                "None of the merged sub-polygons contain the train point")
        target_poly = polys[0]
    elif merged.geom_type == "Polygon":
        target_poly = merged
    else:
        raise ValueError(f"Unexpected geom type: {merged.geom_type}")

    # 6. Only mark railsegments that intersect with the target polygon as inactive
    affected = []
    for seg in segments:
        seg_shape = shape(seg["geometry"])
        if not seg_shape.intersects(target_poly):
            continue

        # Update status to false
        info = seg["fields"].setdefault("info", {})
        info["status"] = False

        # Push update to the server
        client.execute_command(
            "SET", "railsegment", seg["id"],
            "FIELD", "info", json.dumps(info),
            "OBJECT", json.dumps(seg["geometry"])
        )

        # Construct a GeoJSON Feature
        affected.append({
            "type": "Feature",
            "geometry": seg["geometry"],
            "properties": info
        })

    logger.info(f"🔨 Updated {len(affected)} railsegments to inactive")

    # 7. Return the polygon that contains the train point and affected segments
    merged_geojson = mapping(target_poly)
    return merged_geojson, affected


def get_trains_within_area(client):
    """
    Returns a list of train IDs with is_within_area=True
    """
    try:
        response = client.execute_command("SCAN", "train")
        items = response[1] if len(response) > 1 else []
        train_ids = []

        for item in items:
            # 取出 ID
            if isinstance(item, list) and item:
                train_id = item[0]
                fields = item[2] if len(item) > 2 else []
            else:
                train_id = item
                fields = None

            if isinstance(train_id, bytes):
                train_id = train_id.decode()

            info_obj = None
            try:
                if fields and isinstance(fields, list) and len(fields) >= 2:
                    info_obj = json.loads(fields[1])
                else:
                    resp = client.execute_command(
                        "GET", "train", train_id, "WITHFIELDS", "OBJECT")
                    if resp and len(resp) > 1 and len(resp[1]) >= 2:
                        info_obj = json.loads(resp[1][1])
            except Exception as e:
                logger.warning(f"❌ Error parsing info for {train_id}: {e}")

            if info_obj and info_obj.get("is_within_area") is True:
                train_ids.append(train_id)

        logger.info(
            f"✅ Found {len(train_ids)} trains within area: {train_ids}")
        return train_ids

    except Exception as e:
        logger.error(f"🚨 Failed to fetch trains in area: {e}", exc_info=True)
        return []


def fetch_and_freeze_train(client, train_id):
    response = client.execute_command(
        "GET", "train", train_id, "WITHFIELDS", "OBJECT")
    if response is None or len(response) < 2:
        raise ValueError(
            f"Unexpected train response for ID {train_id}: {response}")

    geometry_obj = json.loads(response[0])
    field_data = json.loads(response[1][1]) if len(response[1]) >= 2 else {}

    field_data["status"] = False
    field_data["frozen_coords"] = geometry_obj["coordinates"]

    train_obj = {
        "ok": True,
        "object": geometry_obj,
        "fields": {"info": field_data}
    }

    # SET updated train back to Tile38
    args = ["SET", "train", train_id]
    args += ["FIELD", "info", json.dumps(field_data)]
    args += ["OBJECT", json.dumps(geometry_obj)]
    client.execute_command(*args)
    logger.info(f"🧊 Train {train_id} frozen and updated.")

    return train_obj


# def update_nearby_segments(client, train_obj):
#     coords = train_obj["object"]["coordinates"]
#     if train_obj["object"]["type"] != "Point" or len(coords) < 2:
#         raise ValueError(f"Invalid train geometry: {coords}")

#     lon, lat = coords[:2]
#     response = client.execute_command(
#         "NEARBY", "railsegment", "POINT", lat, lon, 1000)
#     items = response[1] if len(response) > 1 else []

#     rail_ids = []
#     for item in items:
#         if isinstance(item, list) and item:
#             rail_id = item[0]
#             if isinstance(rail_id, bytes):
#                 rail_id = rail_id.decode()
#             rail_ids.append(rail_id)

#     logger.info(f"🛤️ Found {len(rail_ids)} nearby segments.")

#     affected = []
#     for rail_id in rail_ids:
#         resp = client.execute_command(
#             "GET", "railsegment", rail_id, "WITHFIELDS", "OBJECT")
#         if not resp or len(resp) < 2:
#             continue

#         geom = json.loads(resp[0])
#         fields = json.loads(resp[1][1]) if len(resp[1]) >= 2 else {}
#         fields.setdefault("info", {})["status"] = False

#         # Update back to server
#         args = ["SET", "railsegment", rail_id, "FIELD", "info",
#                 json.dumps(fields), "OBJECT", json.dumps(geom)]
#         client.execute_command(*args)

#         affected.append({
#             "type": "Feature",
#             "geometry": geom,
#             "properties": fields.get("info", {})
#         })

#     return affected


# def merge_segments_to_zone(segments):
#     if not segments:
#         raise ValueError("No segments to merge.")

#     shapes = [shape(seg["geometry"]) for seg in segments]
#     merged = unary_union(shapes)
#     geojson = json.loads(json.dumps(merged.__geo_interface__))

#     logger.info(f"📐 Merged zone created: {merged.geom_type}")
#     return geojson


def create_hooks(client, geojson_zone):
    for collection in ["train", "ambulance"]:
        try:
            hook_name = f"{collection}_alert_zone"
            client.execute_command(
                "SETCHAN", hook_name,
                "WITHIN", collection,
                "FENCE", "DETECT", "enter, inside, exit",
                "COMMANDS", "set",
                "OBJECT", json.dumps(geojson_zone)
            )
            logger.info(f"📡 Hook created for {collection}")
        except Exception as e:
            logger.warning(f"⚠️ Hook setup failed for {collection}: {e}")


def reemit_entities(client, collections):
    for collection in collections:
        response = client.execute_command("SCAN", collection)
        items = response[1] if len(response) > 1 else []
        logger.info(f"🔁 Re-emitting {len(items)} in {collection}")

        # Determine the Redis set that tracks who is in the geofence
        inside_set_key = f"{collection}_geofence_inside"

        for item in items:
            entity_id = item[0] if isinstance(item, list) else item
            if isinstance(entity_id, bytes):
                entity_id = entity_id.decode()

            resp = client.execute_command(
                "GET", collection, entity_id, "WITHFIELDS", "OBJECT")
            if not resp or len(resp) < 2:
                continue

            geom = json.loads(resp[0])
            fields = json.loads(resp[1][1]) if len(resp[1]) >= 2 else {}

            # Determine actual in_geofence status using Redis Set
            is_inside = redis_client.sismember(inside_set_key, entity_id)
            fields["in_geofence"] = bool(is_inside)

            args = [
                "SET", collection, entity_id,
                "FIELD", "info", json.dumps(fields),
                "OBJECT", json.dumps(geom)
            ]

            client.execute_command(*args)


# Geofence message handler
def handle_geofence_message(message):
    if message["type"] != "pmessage":
        return

    try:
        channel = message["channel"]
        data_raw = message["data"]

        if isinstance(data_raw, bytes):
            data_raw = data_raw.decode()

        data = json.loads(data_raw)

        # 🧠 Get collection before checking 'status'
        if isinstance(channel, bytes):
            channel = channel.decode()

        match = re.match(r"(train|ambulance)_alert_zone", channel)
        collection = match.group(1) if match else None
        if not collection:
            logger.warning(f"⚠️ Could not determine collection from channel: {channel}")
            return

        info = data.get("fields", {}).get("info", {})

        # 🚦 Only skip if it's a train with status False
        if collection == "train" and info.get("status") is False:
            logger.info(f"🛑 Skipping geofence event for inactive train: {info}")
            return

        logger.info(f"📥 Raw geofence data received on {channel}: {json.dumps(data)}")

        entity_id = data.get("id") or data.get("object", {}).get("id")
        if not entity_id:
            logger.info("⚠️ Skipping geofence message — no entity ID found.")
            return

        key = f"{collection}_alerts"
        inside_once_key = f"{collection}_inside_once"
        inside_set_key = f"{collection}_geofence_inside"
        event = data.get("detect")
        entity_type = collection.capitalize()

        message_text = None

        if event == "enter":
            redis_client.sadd(inside_set_key, entity_id)
            redis_client.srem(inside_once_key, entity_id)
            message_text = f"{entity_type} {entity_id} entered the geofenced area."

        elif event == "inside":
            redis_client.sadd(inside_set_key, entity_id)
            if not redis_client.sismember(inside_once_key, entity_id):
                redis_client.sadd(inside_once_key, entity_id)
                message_text = f"{entity_type} {entity_id} is inside the geofenced area."

        elif event == "exit":
            redis_client.srem(inside_set_key, entity_id)
            redis_client.srem(inside_once_key, entity_id)
            message_text = f"{entity_type} {entity_id} exited the geofenced area."

        else:
            logger.warning(f"⚠️ Unknown event '{event}' on channel: {channel}")
            return

        # 📝 Update 'in_geofence' field in Redis
        try:
            in_geofence = event in ("enter", "inside")
            redis_client.hset(f"{collection}:{entity_id}", "in_geofence", json.dumps(in_geofence))
            logger.info(f"📝 Stored 'in_geofence' = {in_geofence} for {collection}:{entity_id} in Redis")
        except Exception as update_error:
            logger.warning(f"⚠️ Failed to store 'in_geofence' for {entity_id} in Redis: {update_error}")

        if message_text:
            redis_client.lpush(key, message_text)
            redis_client.ltrim(key, 0, 19)

        asyncio.run_coroutine_threadsafe(
            broadcast_queue.put({
                "type": "geofence_alert",
                "collection": collection,
                "segment": channel,
                "entity_id": entity_id,
                "data": data
            }),
            asyncio.get_event_loop()
        )

    except Exception as e:
        logger.error(f"❌ Error handling geofence message: {e}", exc_info=True)



def start_geofence_listener():
    def listen():
        # Create a new event loop for this thread
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)

        pubsub = client.pubsub()  # ✅ use Tile38 client
        pubsub.psubscribe("train_alert_zone", "ambulance_alert_zone")
        logger.info(
            "🛰️ Tile38 geofence listener started and subscribed to *_alert_zone channels")

        for message in pubsub.listen():
            logger.info(f"📨 PubSub message received: {message}")
            if message["type"] == "pmessage":
                handle_geofence_message(message)

    thread = threading.Thread(target=listen, daemon=True)
    thread.start()


def fetch_entity_positions(collection: str):
    """
    Fetches position data for a given entity type from Redis Tile38.

    Parameters:
    - collection (str): The name of the collection (e.g., 'train', 'ambulance').

    Returns:
    - List[dict]: A list of dictionaries containing entity position data.
    """
    positions = []
    try:
        cursor, response = client.execute_command("SCAN", collection)
        for entry in response:
            try:
                obj_id, geojson_str, info_str = entry[0], entry[1], entry[2][1]

                geojson_obj = json.loads(geojson_str)
                info_obj = json.loads(info_str)

                coords = geojson_obj.get("coordinates", [])

                # Override coordinates if status is False and frozen_coords is available
                if info_obj.get("status") is False and "frozen_coords" in info_obj:
                    coords = info_obj["frozen_coords"]

                if len(coords) < 2:
                    continue

                timestamp = coords[2] if len(coords) > 2 else None

                # Ensure availability_status is present
                if "availability_status" not in info_obj:
                    info_obj["availability_status"] = True

                positions.append({
                    "lat": coords[1],
                    "lng": coords[0],
                    "id": obj_id,
                    "timestamp": timestamp,
                    "info": info_obj,
                })
            except json.JSONDecodeError:
                print(f"[ERROR] Failed to decode JSON data for entry: {entry}")
            except Exception as e:
                print(f"[ERROR] Unexpected error parsing entry {entry}: {e}")
    except Exception as e:
        print(f"[ERROR] SCAN failed for {collection}: {e}")

    return positions


async def get_all_positions():
    """
    Fetches position data for trains and ambulances.

    Returns:
    - dict: Dictionary containing lists of train and ambulance positions.
    """
    return {
        "train": fetch_entity_positions("train"),
        "ambulance": fetch_entity_positions("ambulance"),
    }


async def send_positions(websocket: WebSocket):
    """
    Continuously fetches and sends position data via WebSocket.

    Parameters:
    - websocket (WebSocket): FastAPI WebSocket connection.
    """
    try:
        while True:
            positions = await get_all_positions()
            await websocket.send_text(json.dumps(positions))
            await asyncio.sleep(1)  # Update rate: 1 second
    except Exception as e:
        print(f"[ERROR] WebSocket transmission error: {e}")
        await websocket.close()


def create_incident(client, train_id, location, description="Incident reported", train_timestamp=None):
    logger = logging.getLogger(__name__)

    incident_id = f"incident_{train_id}_{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"

    SEVERITY_DESCRIPTION_MAP = {
        "low": [
            "Passenger with mild nausea",
            "Passenger feeling light-headed",
            "Complaint of headache",
            "Minor allergic reaction"
        ],
        "moderate": [
            "Injuries while boarding",
            "Passengers fainted",
            "Small fire and burnt passengers"
        ],
        "high": [
            "Passengers showing signs of stroke",
            "Panic, epileptic and diabetic attacks"
        ],
        "critical": [
            "Multiple injuries from collision",
            "Small fire and burnt passengers"
        ]
    }

    # Choose severity and corresponding description
    severity = random.choice(list(SEVERITY_DESCRIPTION_MAP.keys()))
    description = random.choice(SEVERITY_DESCRIPTION_MAP[severity])

    # Determine timestamp
    incident_time = train_timestamp or datetime.datetime.utcnow().isoformat() + "Z"

    # Additional contextual fields
    affected_passengers_map = {
        "low": 1,
        "moderate": 3,
        "high": 5,
        "critical": 12
    }
    ambulance_units_map = {
        "low": 1,
        "moderate": 2,
        "high": 3,
        "critical": 4
    }
    expected_resolving_time_map = { 
        "low": 5, 
        "moderate": 10, 
        "high": 20, 
        "critical": 30 
    } 

    affected_passengers = affected_passengers_map[severity]
    ambulance_units = ambulance_units_map[severity]
    expected_resolving_time = expected_resolving_time_map[severity] 
    technical_resources = "no"  # Always "no"

    if not isinstance(location, dict) or "type" not in location or "coordinates" not in location:
        logger.error(
            f"Invalid location data passed to create_incident: {location}")
        return None

    try:
        geometry_json = json.dumps(location)
        client.execute_command(
            "SET", "broken_train", incident_id,
            "FIELD", "severity", severity,
            "FIELD", "description", description,
            "FIELD", "status", "inactive",
            "FIELD", "timestamp", str(incident_time),
            "FIELD", "affected_passengers", affected_passengers,
            "FIELD", "ambulance_units", ambulance_units,
            "FIELD", "technical_resources", technical_resources,
            "FIELD", "expected_resolving_time", expected_resolving_time, 
            "OBJECT", geometry_json
        )

        logger.info(f"Created incident {incident_id} for train {train_id}.")

        return {
            "incident_id": incident_id,
            "train_id": train_id,
            "location": location,
            "severity": severity,
            "description": description,
            "timestamp": incident_time,
            "affected_passengers": affected_passengers,
            "ambulance_units": ambulance_units,
            "technical_resources": technical_resources,
            "expected_resolving_time": expected_resolving_time,
        }
    except Exception as e:
        logger.error(
            f"Failed to create incident {incident_id}: {e}", exc_info=True)
        return None


def reset_all_trains(client):
    """Reset all trains to active status and clear incidents"""
    try:
        # Reset all trains
        response = client.execute_command("SCAN", "train")
        cursor = response[0]
        items = response[1] if len(response) > 1 else []

        for item in items:
            try:
                train_id = item[0]
                response = client.execute_command(
                    "GET", "train", train_id, "WITHFIELDS", "OBJECT")

                if response is None:
                    continue

                geometry_obj = json.loads(response[0])
                fields = {}

                if len(response) > 1:
                    field_key = response[1][0]
                    field_value_str = response[1][1]
                    fields = json.loads(field_value_str)

                # Reset status and clear accident location
                fields["status"] = True
                fields.pop("in_geofence", None)
                fields.pop("accident_location", None)

                # Update in Tile38
                client.execute_command(
                    "SET", "train", train_id,
                    "FIELD", "info", json.dumps(fields),
                    "OBJECT", json.dumps(geometry_obj)
                )
                redis_client.hdel(f"train:{train_id}", "in_geofence")
            except Exception as e:
                logging.error(f"Error resetting train {train_id}: {e}")

        # Reactivate all railsegments by setting status = True
        try:
            response = client.execute_command("SCAN", "railsegment")
            cursor = response[0]
            items = response[1] if len(response) > 1 else []

            for item in items:
                try:
                    rail_id = item[0] if isinstance(item, list) else item
                    if isinstance(rail_id, bytes):
                        rail_id = rail_id.decode()

                    rail_get = client.execute_command(
                        "GET", "railsegment", rail_id, "WITHFIELDS", "OBJECT")
                    if rail_get is None or len(rail_get) < 2:
                        continue

                    geometry_obj = json.loads(rail_get[0])
                    fields_data = {}

                    if len(rail_get[1]) >= 2:
                        fields_data = {rail_get[1][0]                                       : json.loads(rail_get[1][1])}

                    if "info" in fields_data and isinstance(fields_data["info"], dict):
                        fields_data["info"]["status"] = True
                    else:
                        fields_data["info"] = {"status": True}

                    # Update railsegment
                    field_args = []
                    for key, value in fields_data.items():
                        field_args.extend(["FIELD", key, json.dumps(value)])

                    client.execute_command(
                        "SET", "railsegment", rail_id,
                        *field_args,
                        "OBJECT", json.dumps(geometry_obj)
                    )
                    # logging.info(f"✅ Reactivated railsegment {rail_id}")
                except Exception as e:
                    logging.warning(
                        f"⚠️ Could not reset railsegment {rail_id}: {e}")
        except Exception as e:
            logging.error(f"❌ Failed to reset railsegments: {e}")

        try:
            response = client.execute_command("SCAN", "ambulance")
            cursor = response[0]
            items = response[1] if len(response) > 1 else []

            for item in items:
                try:
                    amb_id = item[0]
                    response = client.execute_command(
                        "GET", "ambulance", amb_id, "WITHFIELDS", "OBJECT")

                    if response is None:
                        continue

                    geometry_obj = json.loads(response[0])
                    fields = {}

                    if len(response) > 1:
                        field_key = response[1][0]
                        field_value_str = response[1][1]
                        fields = json.loads(field_value_str)

                    # Remove only the in_geofence field if it exists
                    if "in_geofence" in fields:
                        fields.pop("in_geofence")
                        client.execute_command(
                            "SET", "ambulance", amb_id,
                            "FIELD", "info", json.dumps(fields),
                            "OBJECT", json.dumps(geometry_obj)
                        )
                        redis_client.hdel(f"ambulance:{amb_id}", "in_geofence")  # <-- Added this
                        logging.info(f"🧼 Removed 'in_geofence' from ambulance {amb_id}")

                except Exception as e:
                    logging.warning(f"⚠️ Could not clean ambulance {amb_id}: {e}")
        except Exception as e:
            logging.error(f"❌ Failed to scan ambulances: {e}")

        # Clear all incidents
        client.execute_command("DROP", "broken_train")
        client.execute_command("PDELCHAN", "train_alert_zone")
        client.execute_command("PDELCHAN", "ambulance_alert_zone")
        redis_client.delete("train_alerts")
        redis_client.delete("ambulance_alerts")
        logging.info(
            "✅ All trains reset to active status and incidents cleared!")
        return "success"

    except Exception as e:
        logging.error(f"Reset failed: {e}")
        return "fail"



@app.websocket("/ws/scan")
async def scan_websocket(websocket: WebSocket):
    await websocket.accept()
    websocket_connections.add(websocket)

    try:
        request_data = await websocket.receive_text()
        collection = json.loads(request_data).get("collection")

        if not collection:
            await websocket.send_text(json.dumps({"error": "Invalid collection name"}))
            return

        while True:
            all_records = []
            cursor = 0

            try:
                # SCAN all object keys in the collection
                while True:
                    cursor, response = client.execute_command("SCAN", collection, "CURSOR", cursor)
                    all_records.extend(response)
                    if cursor == 0:
                        break

                enriched_records = []

                for item in all_records:
                    entity_id = item[0] if isinstance(item, list) else item
                    if isinstance(entity_id, bytes):
                        entity_id = entity_id.decode()

                    # Get object and fields from Tile38
                    resp = client.execute_command("GET", collection, entity_id, "WITHFIELDS", "OBJECT")
                    if not resp or len(resp) < 2:
                        continue

                    geom = json.loads(resp[0])
                    fields_raw = resp[1][1] if len(resp[1]) >= 2 else "{}"
                    fields = json.loads(fields_raw)

                    # Query Redis for the in_geofence flag
                    redis_key = f"{collection}:{entity_id}"
                    in_geo_raw = redis_client.hget(redis_key, "in_geofence")
                    in_geofence = json.loads(in_geo_raw) if in_geo_raw else False
                    fields["in_geofence"] = in_geofence

                    # Prepare for frontend
                    enriched_records.append([
                        entity_id,
                        json.dumps(geom),
                        ["info", json.dumps(fields)]
                    ])

                # Send enriched data to frontend
                raw_data = {
                    "type": "scan",
                    "collection": collection,
                    "data": enriched_records
                }
                await websocket.send_text(json.dumps(raw_data))

            except Exception as e:
                await websocket.send_text(json.dumps({"error": f"SCAN failed: {e}"}))

            await asyncio.sleep(1)

    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        websocket_connections.discard(websocket)
        await websocket.close()


@app.on_event("startup")
def startup_event():
    start_geofence_listener()
