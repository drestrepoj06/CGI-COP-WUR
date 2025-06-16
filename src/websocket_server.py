from fastapi import FastAPI, WebSocket
import json
import asyncio
import redis
import random
import logging
import datetime

logger = logging.getLogger("uvicorn")
# Initialize FastAPI and Redis client
app = FastAPI()
client = redis.Redis(host="tile38", port=9851, decode_responses=True)

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

                geojson_obj, info_obj = json.loads(geojson_str), json.loads(info_str)
                coords = geojson_obj.get("coordinates", [])

                if len(coords) < 2:
                    continue
                
                timestamp = coords[2] if len(coords) > 2 else None  # Optional timestamp

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




def create_incident(client, train_id, location, description="Incident reported", severity="high", train_timestamp=None):
    # Generate a unique incident ID based on train_id and current time
    incident_id = f"incident_{train_id}_{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"

    # Use train timestamp if provided; else use current UTC time in ISO format
    if train_timestamp:
        # You can convert train_timestamp to ISO string if needed here
        incident_time = train_timestamp
    else:
        incident_time = datetime.datetime.utcnow().isoformat() + "Z"

    # Validate location is GeoJSON-like dict
    if not isinstance(location, dict) or "type" not in location or "coordinates" not in location:
        logger.error(f"Invalid location data passed to create_incident: {location}")
        return False

    try:
        geometry_json = json.dumps(location)
        
        # Instead of packing all into one field, pass individual fields to Tile38 SET
        # Note: Tile38 fields are strings; convert as needed
        client.execute_command(
            "SET", "incidents", incident_id,
            "FIELD", "train_id", str(train_id),
            "FIELD", "severity", severity,
            "FIELD", "status", "active",
            "FIELD", "timestamp", str(incident_time),
            "FIELD", "description", description,
            "OBJECT", geometry_json
        )
        logger.info(f"Created incident {incident_id} for train {train_id}.")
        return True
    except Exception as e:
        logger.error(f"Failed to create incident {incident_id}: {e}", exc_info=True)
        return False

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

    if not train_ids:
        logger.warning("No trains found to update.")
        return False

    selected_id = random.choice(train_ids)
    logger.info(f"Selected train: {selected_id}")

    try:
        response = client.execute_command("GET", "train", selected_id, "WITHFIELDS", "OBJECT")
        if response is None:
            raise ValueError(f"GET command returned None for train ID {selected_id}")
        
        if isinstance(response, list) and len(response) >= 2:
            geometry_obj = json.loads(response[0])
            
            fields = {}
            if len(response[1]) >= 2:
                field_key = response[1][0]  # 'info'
                field_value_str = response[1][1]  # JSON string
                fields[field_key] = json.loads(field_value_str)
            
            train_obj = {
                "ok": True,
                "object": geometry_obj,
                "fields": fields
            }
        else:
            raise ValueError(f"Unexpected response format: {response}")
            
    except Exception as e:
        logger.error(f"Failed to get object for train {selected_id}: {e}", exc_info=True)
        return False

    fields = train_obj["fields"]
    
    if "info" in fields and isinstance(fields["info"], dict):
        fields["info"]["status"] = False
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

        # Create an incident for this train
        create_incident(client, selected_id, train_obj["object"], description="Train marked inactive due to incident")

    except Exception as e:
        logger.error(f"Failed to update train {selected_id}: {e}", exc_info=True)
        return False

    return True



@app.websocket("/ws/positions")
async def websocket_endpoint(websocket: WebSocket):
    logger.info("WebSocket endpoint entered!")
    await websocket.accept()
    logger.info("WebSocket accepted!")

    try:
        await mark_random_train_as_inactive()
        logger.info("After mark_random_train_as_inactive!")
    except Exception as e:
        logger.error(f"[ERROR] mark_random_train_as_inactive() failed: {e}")

    await send_positions(websocket)



has_marked_train = False

@app.websocket("/ws/scan")
async def scan_websocket(websocket: WebSocket):
    global has_marked_train
    
    # Call mark_random_train_as_inactive() **only once**
    if not has_marked_train:
        try:
            await mark_random_train_as_inactive()
            logger.info("✅ Marked one random train as inactive!")
            has_marked_train = True  # Set flag to True after execution
        except Exception as e:
            logger.error(f"[ERROR] mark_random_train_as_inactive() failed: {e}")


    """
    WebSocket endpoint that continuously scans data and sends updates every second,
    ensuring each full SCAN query is completed before restarting.
    """
    
    print("[INFO] WebSocket connected.")
    await websocket.accept()

    try:
        await mark_random_train_as_inactive()
        logger.info("After mark_random_train_as_inactive!")
    except Exception as e:
        logger.error(f"[ERROR] mark_random_train_as_inactive() failed: {e}")


    try:
        request_data = await websocket.receive_text()  # Receive collection request once
        collection = json.loads(request_data).get("collection")

        if not collection:
            await websocket.send_text(json.dumps({"error": "Invalid collection name"}))
            return

        while True:
            all_records = []
            cursor = 0  # 初始游标

            try:
                # 🚀 **Loop through all pages of SCAN results**
                while True:
                    cursor, response = client.execute_command("SCAN", collection, "CURSOR", cursor)
                    all_records.extend(response)  # Append retrieved data

                    if cursor == 0:  # 🌍 If cursor is 0, all data has been retrieved
                        break

                print(f"[DEBUG] Total records fetched: {len(all_records)}")  # Log full dataset size

                raw_data = {"collection": collection, "data": all_records}  # Prepare data
                await websocket.send_text(json.dumps(raw_data))  # Send data to client

            except Exception as e:
                print(f"[ERROR] SCAN failed for {collection}: {e}")
                await websocket.send_text(json.dumps({"error": f"SCAN failed: {e}"}))

            await asyncio.sleep(1)  # ⏳ **Wait 1 second before starting a new SCAN query**

    except Exception as e:
        print(f"[ERROR] WebSocket error: {e}")
        await websocket.close()
