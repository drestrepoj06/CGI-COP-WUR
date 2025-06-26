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

        if collection == "train":
            redis_client.rpush("train_alerts", entity_id)
            logger.info(f"🚆 Train alert triggered: {entity_id}")
        elif collection == "ambulance":
            redis_client.rpush("ambulance_alerts", entity_id)
            logger.info(f"🚑 Ambulance alert triggered: {entity_id}")
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
                        fields_data = {rail_get[1][0]: json.loads(rail_get[1][1])}

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


def get_trains_within_area(client):
    """
    Fetches all train IDs that have is_within_area=True from Redis Tile38.
    """

    try:
        response = client.execute_command("SCAN", "train")
        cursor = response[0]
        items = response[1] if len(response) > 1 else []

        train_ids = []
        for item in items:
            try:
                if isinstance(item, list) and len(item) > 0:
                    train_id = item[0]
                    info_str = item[2][1] if len(item) > 2 and isinstance(
                        item[2], list) and len(item[2]) > 1 else None

                    if info_str:
                        info_obj = json.loads(info_str)
                        if info_obj.get("is_within_area") is True:
                            if isinstance(train_id, bytes):
                                train_id = train_id.decode()
                            train_ids.append(train_id)
                else:
                    # For non-list items (unlikely case), we need to fetch the full record
                    train_id = item if isinstance(item, str) else item.decode()
                    response = client.execute_command(
                        "GET", "train", train_id, "WITHFIELDS", "OBJECT")

                    if response and len(response) > 1:
                        info_str = response[1][1] if len(
                            response[1]) > 1 else None
                        if info_str:
                            info_obj = json.loads(info_str)
                            if info_obj.get("is_within_area") is True:
                                train_ids.append(train_id)
            except Exception as e:
                logger.warning(f"Error processing train item {item}: {e}")
                continue

        logger.info(f"Found {len(train_ids)} trains within area: {train_ids}")
        return train_ids

    except Exception as e:
        logger.error(f"Error scanning trains: {e}", exc_info=True)
        return []



def mark_random_train_as_inactive(client):
    try:
        response = client.execute_command("SCAN", "train")
        cursor = response[0]
        items = response[1] if len(response) > 1 else []

        train_ids = []
        for item in items:
            if isinstance(item, list) and len(item) > 0:
                train_id = item[0]
                if isinstance(train_id, bytes):
                    train_id = train_id.decode()
                train_ids.append(train_id)
            else:
                if isinstance(item, bytes):
                    train_ids.append(item.decode())
                elif isinstance(item, str):
                    train_ids.append(item)

        logger.info(f"Found {len(train_ids)} trains: {train_ids}")

    except Exception as e:
        logger.error(f"Error scanning trains: {e}", exc_info=True)
        return False

    selected_id = random.choice(train_ids)

    try:
        response = client.execute_command(
            "GET", "train", selected_id, "WITHFIELDS", "OBJECT")

        if response is None:
            raise ValueError(f"GET command returned None for train ID {selected_id}")

        if isinstance(response, list) and len(response) >= 2:
            geometry_obj = json.loads(response[0])
            info_data = {}
            if len(response[1]) >= 2:
                field_key = response[1][0]
                field_value_str = response[1][1]
                info_data = json.loads(field_value_str)

            info_data["status"] = False
            info_data["frozen_coords"] = geometry_obj["coordinates"]

            train_obj = {
                "ok": True,
                "object": geometry_obj,
                "fields": {
                    "info": info_data
                }
            }
        else:
            raise ValueError(f"Unexpected response format: {response}")

    except Exception as e:
        logger.error(f"Failed to get object for train {selected_id}: {e}", exc_info=True)
        return False

    try:
        geometry_json = json.dumps(train_obj["object"])
        fields_args = []
        for field_key, field_value in train_obj["fields"].items():
            fields_args.extend(["FIELD", field_key, json.dumps(field_value)])

        set_args = ["SET", "train", selected_id] + fields_args + ["OBJECT", geometry_json]
        logger.info(f"SET command args: {set_args}")
        client.execute_command(*set_args)
        logger.info(f"Updated train {selected_id} status to False.")

        check_response = client.execute_command(
            "GET", "train", selected_id, "WITHFIELDS", "OBJECT")
        logger.info(f"🔎 Post-update check for {selected_id}: {check_response}")

        coords = train_obj["object"]["coordinates"]
        if train_obj["object"]["type"] != "Point" or len(coords) < 2:
            raise ValueError(f"Invalid train geometry for proximity search: {coords}")

        lon, lat = coords[:2]
        nearby_response = client.execute_command(
            "NEARBY", "railsegment", "POINT", float(lat), float(lon), 1000)

        rail_ids = []
        if isinstance(nearby_response, list) and len(nearby_response) > 1:
            results = nearby_response[1]
            for item in results:
                if isinstance(item, list) and len(item) >= 1:
                    rail_id = item[0]
                    if isinstance(rail_id, bytes):
                        rail_id = rail_id.decode()
                    rail_ids.append(rail_id)

        logger.info(f"Found {len(rail_ids)} nearby railsegments within 1 km: {rail_ids}")
        affected_segments = []

        for rail_id in rail_ids:
            rail_get = client.execute_command(
                "GET", "railsegment", rail_id, "WITHFIELDS", "OBJECT")
            if rail_get is None or len(rail_get) < 2:
                logger.warning(f"Could not retrieve railsegment {rail_id}")
                continue

            geometry_obj = json.loads(rail_get[0])
            fields_data = {}
            if len(rail_get[1]) >= 2:
                fields_data = {rail_get[1][0]: json.loads(rail_get[1][1])}

            fields_data.setdefault("info", {})["status"] = False

            # SET updated segment in Tile38
            field_args = []
            for key, value in fields_data.items():
                field_args.extend(["FIELD", key, json.dumps(value)])

            set_args = ["SET", "railsegment", rail_id] + field_args + ["OBJECT", json.dumps(geometry_obj)]
            client.execute_command(*set_args)
            logger.info(f"Updated railsegment {rail_id} status to False")

            # 🚧 Register geofence hook for this segment
            clean_id = rail_id
            if rail_id.startswith("segment_"):
                clean_id = rail_id[len("segment_"):]

            geometry_only = geometry_obj.get("geometry", geometry_obj)
                        
            info_props = fields_data.get("info", {})
            affected_segments.append({
                "type": "Feature",
                "geometry": geometry_obj,
                "properties": info_props
            })

        # Log how many segments you’re merging
        logger.info(f"🔀 Merging {len(affected_segments)} affected segments into a single geofence zone.")

        try:
            shapely_geoms = [shape(f["geometry"]) for f in affected_segments]
            merged_geom = unary_union(shapely_geoms)
            logger.info(f"✅ Successfully merged geometry type: {merged_geom.geom_type}")
        except Exception as e:
            logger.error(f"❌ Failed to merge geometries: {e}", exc_info=True)
            return

        # Convert to GeoJSON
        try:
            merged_geojson = json.loads(json.dumps(merged_geom.__geo_interface__))
            logger.info(f"🧾 Merged GeoJSON geometry: {json.dumps(merged_geojson)[:300]}...")  # Truncated for readability
        except Exception as e:
            logger.error(f"❌ Failed to convert merged geometry to GeoJSON: {e}", exc_info=True)
            return

        # Create hooks
        for collection in ["train", "ambulance"]:
            try:
                hook_name = f"{collection}_alert_zone"
                client.execute_command(
                    "SETCHAN", hook_name,
                    "WITHIN", collection,
                    "FENCE", "DETECT", "enter, inside, exit",
                    "COMMANDS", "set",
                    "OBJECT", json.dumps(merged_geojson)
                )
                logger.info(f"📡 Hook created: {hook_name} for collection: {collection}")
            except Exception as e:
                logger.error(f"❌ Failed to create geofence hook for {collection}: {e}", exc_info=True)

        # Re-send entities to trigger hooks
        for collection in ["train", "ambulance"]:
            try:
                response = client.execute_command("SCAN", collection)
                cursor = response[0]
                items = response[1] if len(response) > 1 else []
                logger.info(f"🔄 Resending {len(items)} objects in collection: {collection}")

                for item in items:
                    entity_id = item[0] if isinstance(item, list) else item
                    if isinstance(entity_id, bytes):
                        entity_id = entity_id.decode()

                    entity_response = client.execute_command(
                        "GET", collection, entity_id, "WITHFIELDS", "OBJECT"
                    )

                    if entity_response and len(entity_response) >= 2:
                        entity_geometry = json.loads(entity_response[0])

                        # Get field values safely
                        fields = {}
                        if len(entity_response[1]) >= 2:
                            field_key = entity_response[1][0]
                            field_val = entity_response[1][1]
                            try:
                                fields = json.loads(field_val)
                            except Exception:
                                logger.warning(f"⚠️ Could not parse fields for {entity_id}. Skipping complex values.")
                                fields = {}

                        # Start building the SET command
                        set_args = [
                            "SET", collection, entity_id,
                        ]

                        # Safely add field info
                        set_args.append("FIELD")
                        set_args.append("info")

                        # Flatten and serialize properly
                        try:
                            fields_json = json.dumps(fields_data)
                            set_args.append(fields_json)
                        except Exception as e:
                            logger.warning(f"❌ Failed to JSON-encode fields for {entity_id}: {fields_data} — {e}")
                            return  # Skip this entity

                        # Safely serialize geometry
                        try:
                            object_json = json.dumps(entity_geometry)
                            set_args.append("OBJECT")
                            set_args.append(object_json)
                        except Exception as e:
                            logger.warning(f"❌ Failed to JSON-encode geometry for {entity_id}: {entity_geometry} — {e}")
                            return  # Skip this entity

                        # Final command
                        client.execute_command(*set_args)
                        logger.info(f"📤 Re-set entity {entity_id} in collection {collection}")

            except Exception as e:
                logger.warning(f"⚠️ Failed to re-trigger hook for {collection}: {e}", exc_info=True)    


        incident = create_incident(
            client, selected_id, train_obj["object"], description="Train marked inactive due to incident")

        return {
            "incident": incident,
            "inactive_segments": affected_segments
        }

    except Exception as e:
        logger.error(f"Failed to update train {selected_id}: {e}", exc_info=True)
        return None

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
