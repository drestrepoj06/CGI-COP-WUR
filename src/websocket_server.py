from fastapi import FastAPI, WebSocket
import json
import asyncio
import redis
import random
import logging
import datetime
import threading
from collections import deque

broadcast_queue = asyncio.Queue()
websocket_connections = set()
logger = logging.getLogger("uvicorn")
# Initialize FastAPI and Redis client
app = FastAPI()
client = redis.Redis(host="tile38", port=9851, decode_responses=True)
# Redis client
redis_client = redis.Redis(host="localhost", port=9851, decode_responses=True)

# Geofence message handler
def handle_geofence_message(message):
    if message["type"] != "message":
        return

    try:
        data = json.loads(message["data"])

        # 1. Push the train ID into Redis alert list
        train_id = data.get("object", {}).get("id")  # assumes format from Tile38
        if train_id:
            redis_client.rpush("train_alerts", train_id)
            logger.info(f"🚨 Geofence triggered by train {train_id}, stored in Redis.")

        # 2. Broadcast message to WebSocket clients
        asyncio.run_coroutine_threadsafe(
            broadcast_queue.put({
                "type": "geofence_alert",
                "data": data
            }),
            asyncio.get_event_loop()
        )

    except Exception as e:
        logger.error(f"Error handling geofence event: {e}", exc_info=True)

def start_geofence_listener():
    def listen():
        pubsub = redis_client.pubsub()
        pubsub.psubscribe("segment_*")

        for message in pubsub.listen():
            if message['type'] == 'pmessage':
                channel = message['channel'].decode()
                data = json.loads(message['data'])

                # Example format expected from Tile38
                train_id = data.get('id')
                coords = data.get('object', {}).get('coordinates')
                timestamp = datetime.utcnow().isoformat()

                # Save it (e.g. in Redis, global list, or send via websocket)
                alert_data = {
                    "train_id": train_id,
                    "coords": coords,
                    "segment": channel,
                    "timestamp": timestamp
                }

                redis_client.rpush("alert_trains", json.dumps(alert_data))
                logger.info(f"🚨 Train {train_id} entered inactive segment {channel}")

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

    fields = train_obj["fields"]

    if "info" in fields and isinstance(fields["info"], dict):
        fields["info"]["status"] = False
        fields["info"]["frozen_coords"] = train_obj["object"]["coordinates"]
    else:
        logger.warning(f"Train {selected_id} does not have expected info structure")
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

            hook_name = f"hook_segment_{clean_id}"  
            channel_name = f"segment_{clean_id}"
            hook_endpoint = f"http://localhost:9000/hooks/{channel_name}"
            geometry_only = geometry_obj.get("geometry", geometry_obj)

            try:
                client.execute_command("DELHOOK", hook_name)
            except Exception:
                pass  # Ignore if doesn't exist
            
            client.execute_command(
                "SETHOOK", hook_name, hook_endpoint,
                "WITHIN", "train",
                "FENCE", "DETECT", "enter",
                "COMMANDS", "set",
                "WHERE", "info.status", "==", "false",
                "OBJECT", json.dumps(geometry_only)
            )
            logger.info(f"📡 Created geofence hook {hook_name} on channel {channel_name}")

            affected_segments.append({
                "type": "Feature",
                "geometry": geometry_obj,
                "properties": fields_data["info"]
            })

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
                raw_data = {"type": "scan", "collection": collection, "data": all_records}
                await websocket.send_text(json.dumps(raw_data))

                # Send any geofence alerts from the broadcast queue
                while not broadcast_queue.empty():
                    geofence_event = await broadcast_queue.get()
                    await websocket.send_text(json.dumps(geofence_event))

                # PART B: Send list of stored alerting train IDs
                train_alert_ids = redis_client.lrange("train_alerts", 0, -1)
                if train_alert_ids:
                    await websocket.send_text(json.dumps({
                        "type": "train_alert_list",
                        "data": train_alert_ids
                    }))

            except Exception as e:
                await websocket.send_text(json.dumps({"error": f"SCAN failed: {e}"}))

            await asyncio.sleep(1)

    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        websocket_connections.discard(websocket)
        await websocket.close()