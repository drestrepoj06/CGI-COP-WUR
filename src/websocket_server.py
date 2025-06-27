from shapely.geometry import shape
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
from shapely.geometry import shape, MultiPolygon
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
    try:
        # ['1740', '2038', ... all train_ids]
        train_ids = get_trains_within_area(client)
        selected_id = random.choice(train_ids)  # 8843 randomly choose one
        train_obj = fetch_and_freeze_train(client, selected_id)
        # train_obj: {'ok': True, 'object': {'type': 'Point', # 'coordinates': [5.1420507, 52.06762, 1750761907942]},
        # 'fields': {'info': {'type': 'SPR', 'speed': 0.0, 'direction': 118.5, 'status': False, 'is_within_area':
        # True, 'accident_location': None, 'frozen_coords': [5.1420507, 52.06762, 1750761907942]}}}
        affected_segments = update_nearby_segments(client, train_obj)

        merged_geojson = merge_segments_to_zone(affected_segments)
        create_hooks(client, merged_geojson)
        reemit_entities(client, ["train", "ambulance"])

        logging.info(f"train_ids: {train_ids}")

        logging.info(f"affected_segments: {affected_segments}")
        logging.info(f"merged_geojson: {merged_geojson}")

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


def update_nearby_segments(client, train_obj):
    coords = train_obj["object"]["coordinates"]
    if train_obj["object"]["type"] != "Point" or len(coords) < 2:
        raise ValueError(f"Invalid train geometry: {coords}")

    lon, lat = coords[:2]
    response = client.execute_command(
        "NEARBY", "railsegment", "POINT", lat, lon, 1000)
    items = response[1] if len(response) > 1 else []

    rail_ids = []
    for item in items:
        if isinstance(item, list) and item:
            rail_id = item[0]
            if isinstance(rail_id, bytes):
                rail_id = rail_id.decode()
            rail_ids.append(rail_id)

    logger.info(f"🛤️ Found {len(rail_ids)} nearby segments.")

    affected = []
    for rail_id in rail_ids:
        resp = client.execute_command(
            "GET", "railsegment", rail_id, "WITHFIELDS", "OBJECT")
        if not resp or len(resp) < 2:
            continue

        geom = json.loads(resp[0])
        fields = json.loads(resp[1][1]) if len(resp[1]) >= 2 else {}
        fields.setdefault("info", {})["status"] = False

        # Update back to server
        args = ["SET", "railsegment", rail_id, "FIELD", "info",
                json.dumps(fields), "OBJECT", json.dumps(geom)]
        client.execute_command(*args)

        affected.append({
            "type": "Feature",
            "geometry": geom,
            "properties": fields.get("info", {})
        })

    return affected


def merge_segments_to_zone(segments):
    if not segments:
        raise ValueError("No segments to merge.")

    shapes = [shape(seg["geometry"]) for seg in segments]
    merged = unary_union(shapes)
    geojson = json.loads(json.dumps(merged.__geo_interface__))

    logger.info(f"📐 Merged zone created: {merged.geom_type}")
    return geojson


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

            args = ["SET", collection, entity_id, "FIELD", "info",
                    json.dumps(fields), "OBJECT", json.dumps(geom)]
            client.execute_command(*args)
            # logger.info(f"🛰️ Resent entity {entity_id} in {collection}")


# Geofence message handler
def handle_geofence_message(message):
    if message["type"] != "pmessage":
        return

    try:
        channel = message["channel"]
        data_raw = message["data"]

        # Decode if bytes
        if isinstance(data_raw, bytes):
            data_raw = data_raw.decode()

        data = json.loads(data_raw)
        logger.info(
            f"📥 Raw geofence data received on {channel}: {json.dumps(data)}")

        entity_id = data.get("id") or data.get("object", {}).get("id")
        if not entity_id:
            logger.info("⚠️ Skipping geofence message — no entity ID found.")
            return

        # Try to infer collection from the channel
        if isinstance(channel, bytes):
            channel = channel.decode()
        match = re.match(r"(train|ambulance)_alert_zone", channel)
        collection = match.group(1) if match else None

        key = f"{collection}_alerts"
        event = data.get("detect")

        if event in ["enter", "inside"]:
            redis_client.sadd(key, entity_id)  # Add to Set
        elif event == "exit":
            redis_client.srem(key, entity_id)
        else:
            logger.warning(f"⚠️ Unknown collection/channel: {channel}")

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
        "critical": 5
    }

    affected_passengers = affected_passengers_map[severity]
    ambulance_units = ambulance_units_map[severity]
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
            "technical_resources": technical_resources
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
                if "accident_location" in fields:
                    del fields["accident_location"]

                # Update in Tile38
                client.execute_command(
                    "SET", "train", train_id,
                    "FIELD", "info", json.dumps(fields),
                    "OBJECT", json.dumps(geometry_obj)
                )

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
                        fields_data = {rail_get[1][0]
                            : json.loads(rail_get[1][1])}

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
                    logging.info(f"✅ Reactivated railsegment {rail_id}")
                except Exception as e:
                    logging.warning(
                        f"⚠️ Could not reset railsegment {rail_id}: {e}")

        except Exception as e:
            logging.error(f"❌ Failed to reset railsegments: {e}")

        # Clear all incidents
        client.execute_command("DROP", "broken_train")
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
                # SCAN all objects in the collection
                while True:
                    cursor, response = client.execute_command(
                        "SCAN", collection, "CURSOR", cursor)
                    all_records.extend(response)
                    if cursor == 0:
                        break

                # Send the scanned data
                raw_data = {"type": "scan",
                            "collection": collection, "data": all_records}
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
