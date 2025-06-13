from fastapi import FastAPI, WebSocket
import json
import asyncio
import redis

app = FastAPI()
client = redis.Redis(host="tile38", port=9851, decode_responses=True)


def get_positions_for_entity(collection: str):
    positions = []
    try:
        cursor, response = client.execute_command("SCAN", collection)
        for entry in response:
            try:
                obj_id = entry[0]
                geojson_str = entry[1]
                info_str = entry[2][1]

                geojson_obj = json.loads(geojson_str)
                info_obj = json.loads(info_str)

                coords = geojson_obj.get("coordinates", [])
                if len(coords) < 2:
                    continue
                
                ts = coords[2] if len(coords) > 2 else None  # optional

                positions.append({
                    'lat': coords[1],
                    'lng': coords[0],
                    'id': obj_id,
                    'timestamp': ts,
                    'info': info_obj,
                })
            except Exception as e:
                print(f"[ERROR] Parsing entry {entry}: {e}")
    except Exception as e:
        print(f"[ERROR] SCAN failed for {collection}: {e}")
    return positions



async def fetch_positions():
    train_positions = get_positions_for_entity("train")
    ambulance_positions = get_positions_for_entity("ambulance")

    return {
        'train': train_positions,
        'ambulance': ambulance_positions
    }


@app.websocket("/ws/positions")
async def websocket_endpoint(websocket: WebSocket):
    print("[INFO] WebSocket connected.")
    await websocket.accept()

    try:
        while True:
            positions = await fetch_positions()
            await websocket.send_text(json.dumps(positions))
            await asyncio.sleep(1)  # update rate: 1 second
    except Exception as e:
        print(f"[ERROR] WebSocket error: {e}")
        await websocket.close()
