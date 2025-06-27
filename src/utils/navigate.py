import json
import asyncio
import redis
import logging
import requests
from itertools import cycle

logger = logging.getLogger("uvicorn")
client = redis.Redis(host="tile38", port=9851, decode_responses=True)


def clear_ambu_path_and_broken_train(client):
    try:
        client.execute_command("DROP", "ambu_path2train")
        logging.info("✅ Cleared ambu_path2train collection.")

        client.execute_command("DROP", "broken_train")
        logging.info("✅ Cleared broken_train collection.")

        return "success"
    except Exception as e:
        logging.error(f"❌ Failed to clear collections: {e}")
        return "fail"


def fetch_ambulance_positions():
    """
    Fetches ambulance location data and returns two lists:
    - spare_ambu_positions: ambulances with both availability_status and status as True
    - busy_ambu_positions: all other ambulances
    """
    spare_ambu_positions = []
    busy_ambu_positions = []

    try:
        cursor, response = client.execute_command("SCAN", "ambulance")
        if not response or not isinstance(response, list) or len(response) < 2:
            logger.error(f"Ambulance data not found: {response}")
            return spare_ambu_positions, busy_ambu_positions

        for obj in response:
            # logging.info(f"obj: {obj}")
            try:
                ambu_info = json.loads(obj[2][1])
                ambu_id = ambu_info.get("vehicle_number", [])
                geojson_obj = json.loads(obj[1])
                coords = geojson_obj.get("coordinates", [])

                if len(coords) < 2:
                    continue

                ambu_data = {
                    "id": ambu_id,
                    "lat": coords[1],
                    "lng": coords[0]
                }

                if ambu_info.get("availability_status") is True and ambu_info.get("status") is True:
                    spare_ambu_positions.append(ambu_data)
                else:
                    busy_ambu_positions.append(ambu_data)

            except Exception as e:
                logger.error(f"Error parsing object {obj}: {e}")
    except Exception as e:
        logger.error(f"SCAN `ambulance` failed: {e}")

    return spare_ambu_positions, busy_ambu_positions


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



TOMTOM_API_KEY_LIST = [
    "DJfZGAsAfoE8Nsb6KaOZ1UHXlU7z9sR5",
    "fGPPGPPeZr5bWeBJke1HopjWnkFpk8En",
    "YjyodVbTYnjdjl3JSUzoug3XwpQNuLYi",
    "vsYoRZiUUGpAvPgLZoehtfQ3mlIVCkwv"
]

# JFmPPMpbDhq3WVWuP9494g30V7IyMT6b Xiaolu
# DJfZGAsAfoE8Nsb6KaOZ1UHXlU7z9sR5
# btm27pOSiKOd9tq5EHQvJdsdqj4X4q0C Biden (DEAD for today)

# YjyodVbTYnjdjl3JSUzoug3XwpQNuLYi Falco Latour
# fGPPGPPeZr5bWeBJke1HopjWnkFpk8En Oclaf Ruotal
# vsYoRZiUUGpAvPgLZoehtfQ3mlIVCkwv Mat Mat

API_KEY_ITERATOR = cycle(TOMTOM_API_KEY_LIST)

def get_route(origin, destination):
    NEXT_API_KEY = next(API_KEY_ITERATOR)
    url = (
        f"https://api.tomtom.com/routing/1/calculateRoute/"
        f"{origin[0]},{origin[1]}:{destination[0]},{destination[1]}/json"
        f"?key={NEXT_API_KEY}&travelMode=car&routeType=fastest"
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
