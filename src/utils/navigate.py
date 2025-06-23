import json
import asyncio
import redis
import logging
import requests

logger = logging.getLogger("uvicorn")
client = redis.Redis(host="tile38", port=9851, decode_responses=True)


def fetch_ambulance_positions():
    """
    Fetches ambulance location data and returns only ID and coordinates.

    Returns:
    - List[dict]: A list of dictionaries containing only ambulance ID and coordinates.
    """
    positions = []
    try:
        cursor, response = client.execute_command("SCAN", "ambulance")
        if not response or not isinstance(response, list) or len(response) < 2:
            logger.error(f"Ambulance data not found: {response}")
            return positions

        for obj in response:  # The actual object list
            try:
                ambu_info = json.loads(obj[2][1])  # Ambulance ID (序号)
                ambu_id = ambu_info.get("vehicle_number", [])
                geojson_obj = json.loads(obj[1])  # 解析为 JSON
                coords = geojson_obj.get("coordinates", [])

                if len(coords) < 2:
                    continue

                positions.append({
                    "id": ambu_id,
                    "lat": coords[1],
                    "lng": coords[0]
                })
            except Exception as e:
                logger.error(f"Error parsing object {obj}: {e}")
    except Exception as e:
        logger.error(f"SCAN `ambulance` failed: {e}")

    return positions


def fetch_broken_trains():
    """
    Fetches broken train location data and returns only train ID, coordinates, and timestamp.

    Returns:
    - List[dict]: A list of dictionaries containing only train ID, latitude, longitude, and timestamp.
    """
    broken_trains = []
    try:
        cursor, response = client.execute_command("SCAN", "broken_train")
        if not response or not isinstance(response, list) or len(response) < 2:
            logger.error(f"No broken train data found: {response}")
            return broken_trains

        for obj in response:  # The actual object list
            try:
                broken_train_id = obj[0].split("_")[1]  # Unique incident ID
                geojson_obj = json.loads(obj[1])  # Parse JSON
                coords = geojson_obj.get("coordinates", [])

                broken_trains.append({
                    "train_id": broken_train_id,
                    "severity": obj[2][1],
                    "lat": coords[1],
                    "lng": coords[0],
                    "timestamp": coords[2],
                })
            except Exception as e:
                logger.error(f"Error parsing object {obj}: {e}")
    except Exception as e:
        logger.error(f"SCAN `broken_train` failed: {e}")

    return broken_trains


async def fetch_ambu_broken_train_positions(websocket=None):
    """
    Fetches raw ambulance and broken train data without parsing.

    Parameters:
    - websocket (WebSocket, optional): If provided, sends the data via WebSocket.

    Returns:
    - dict: Raw JSON response from Redis.
    """
    response_data = {
        "ambulance": fetch_ambulance_positions(),
        "broken_trains": fetch_broken_trains(),
    }

    if websocket:
        try:
            await websocket.send_text(json.dumps(response_data))
        except Exception as e:
            logger.error(f"[ERROR] WebSocket transmission error: {e}")
            await websocket.close()

    return response_data

TOMTOM_API_KEY = "btm27pOSiKOd9tq5EHQvJdsdqj4X4q0C"


def get_route(origin, destination):
    url = (
        f"https://api.tomtom.com/routing/1/calculateRoute/"
        f"{origin[0]},{origin[1]}:{destination[0]},{destination[1]}/json"
        f"?key={TOMTOM_API_KEY}&travelMode=car&routeType=fastest"
    )
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Error fetching route: {e}")
        return []


async def calculate_optimal_path(positions):
    if not positions["broken_trains"]:
        return {}

    # 找出时间戳最大的故障列车
    broken_trains = positions["broken_trains"]

    # 如果只有一条记录，就直接使用它；否则，用 max 找出时间戳最大的记录
    latest_train = broken_trains[0] if len(broken_trains) == 1 else max(
        broken_trains, key=lambda t: t["timestamp"])

    # 计算每辆救护车到该故障列车的路径
    ambulance_routes = [
        {
            "timestamp": latest_train["timestamp"],
            "ambulance_id": ambu["id"],
            "route_points": get_route((ambu["lat"], ambu["lng"]), (latest_train["lat"], latest_train["lng"]))["routes"][0]["legs"][0]["points"],
            "route_estimated_time": get_route((ambu["lat"], ambu["lng"]), (latest_train["lat"], latest_train["lng"]))["routes"][0]["legs"][0]["summary"]["travelTimeInSeconds"],
        }
        for ambu in positions["ambulance"]
    ]

    optimal_route = min(
        ambulance_routes, key=lambda r: r["route_estimated_time"])

    return optimal_route


# def fetch_and_display_positions():
#     """

#     Fetch ambulance and broken train positions and display them in the dashboard.
#     """
#     positions = fetch_ambu_broken_train_positions()
#     routes = calculate_optimal_path(positions)

#     st.json(routes)  # Display the fetched data in JSON format

# async def delayed_fetch():
#     """等待 2 秒后执行 fetch_and_display_positions"""
#     await asyncio.sleep(2)
#     await fetch_and_display_positions()


#         # Fetch and display positions below the map
#         asyncio.run(delayed_fetch())
