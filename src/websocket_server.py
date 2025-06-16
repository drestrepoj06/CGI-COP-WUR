from fastapi import FastAPI, WebSocket
import json
import asyncio
import redis

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

@app.websocket("/ws/positions")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time position updates.

    Parameters:
    - websocket (WebSocket): FastAPI WebSocket connection.
    """
    print("[INFO] WebSocket connected.")
    await websocket.accept()
    await send_positions(websocket)


@app.websocket("/ws/scan")
async def scan_websocket(websocket: WebSocket):
    """
    WebSocket endpoint that directly executes SCAN from the Redis client 
    and returns raw data in dictionary format without filtering.
    """
    print("[INFO] WebSocket connected.")
    await websocket.accept()

    try:
        while True:
            message = await websocket.receive_text()
            request_data = json.loads(message)
            collection = request_data.get("collection")

            if collection:
                all_records = []
                cursor = 0  # 初始游标

                try:
                    # 通过 CURSOR 循环获取所有数据
                    while True:
                        cursor, response = client.execute_command("SCAN", collection, "CURSOR", cursor)
                        all_records.extend(response)  # 累加获取的数据

                        if cursor == 0:  # 游标为 0，表示所有数据已返回，退出循环
                            break

                    print(f"[DEBUG] Total records fetched: {len(all_records)}")  # 记录日志

                    raw_data = {"collection": collection, "data": all_records}  # 返回完整数据

                except Exception as e:
                    print(f"[ERROR] SCAN failed for {collection}: {e}")
                    raw_data = {"error": f"SCAN failed: {e}"}

                await websocket.send_text(json.dumps(raw_data))
            else:
                await websocket.send_text(json.dumps({"error": "Invalid collection name"}))

    except Exception as e:
        print(f"[ERROR] WebSocket error: {e}")
        await websocket.close()