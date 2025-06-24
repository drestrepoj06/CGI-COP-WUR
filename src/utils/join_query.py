from streamlit_autorefresh import st_autorefresh
import asyncio
import redis
import json
import logging
from datetime import datetime

import streamlit as st

# Tile38 setup
TILE38_HOST = 'tile38'
TILE38_PORT = 9851


async def record_ambulance_path(route_points, timestamp, route_estimated_time, ambulance_id):

    tile38_client = redis.Redis(
        host=TILE38_HOST, port=TILE38_PORT, decode_responses=True)

    # 遍历所有路径点，并计算时间偏移量
    for index, (lat, lng) in enumerate(route_points):
        adjusted_timestamp = timestamp + \
            (index * route_estimated_time // len(route_points) * 50)

        # 执行 Tile38 SET 命令
        command = f"SET ambu_path {ambulance_id}_{adjusted_timestamp} POINT {lat} {lng} {adjusted_timestamp}"

        # command = f"SET ambu_path {ambulance_id} POINT {lat} {lng} {adjusted_timestamp}"
        tile38_client.execute_command(command)

        print(f"Recorded: {command}")


def display_rescue_progress_auto(client):
    # Refresh every 1 second
    st_autorefresh(interval=1000, key="rescue_polling")

    progress_data = get_ambulance_progress(client)

    if progress_data:
        # st.markdown("### ⏱️ Rescue Progress")
        for ambu in progress_data:
            ambulance_id = ambu["ambulance_id"]
            eta = ambu["eta"]
            travel_time = ambu["travel_time"]
            past_time = ambu["past_time"]

            percent = min(past_time / travel_time, 1.0) if travel_time else 0.0
            remaining_seconds = max(travel_time - past_time, 0)

            # Convert remaining time into minutes and seconds
            minutes = int(remaining_seconds // 60)
            seconds = int(remaining_seconds % 60)

            # Format ETA
            eta_readable = datetime.fromtimestamp(
                eta / 1000).strftime('%Y-%m-%d %H:%M:%S')

            st.markdown(
                f"**🚑 Ambulance {ambulance_id}**  \n"
                f"ETA: {eta_readable} | Remaining Time: {minutes}m {seconds}s"
            )

            st.progress(percent)
    # else:
    #     st.info("Waiting for ambulance path data...")


def get_ambulance_progress(client):
    """
    Retrieve progress status of all ambulances in the ambu_path2train collection.

    Returns:
        List of dictionaries, each containing:
        - ambulance_id (str)
        - past_time (float)
        - travel_time (int or float)
    """
    results = []

    try:
        # logging.info("🚥 Scanning ambu_path2train collection...")
        response = client.execute_command("SCAN", "ambu_path2train")
        items = response[1] if len(response) > 1 else []

        # logging.info(f"📦 Found {len(items)} entries in ambu_path2train.")

        if not items:
            # logging.info("❗ No ambulance route records found.")
            return results

        for item in items:
            try:
                object_id = item[0] if isinstance(item, list) else item
                # logging.info(
                #     f"🔍 Fetching route data for object ID: {object_id}")

                get_response = client.execute_command(
                    "GET", "ambu_path2train", object_id, "WITHFIELDS"
                )
                if not get_response or len(get_response) < 2:
                    logging.warning(
                        f"⚠️ Missing data for {object_id}. Skipping.")
                    continue

                geometry = get_response[0]
                field_data = get_response[1]

                field_dict = dict(zip(field_data[::2], field_data[1::2]))
                info = json.loads(field_dict.get("info", "{}"))

                ambulance_id = str(info.get("ambulance_id"))
                travel_time = info.get("travel_time")
                points = info.get("route_points_timed", [])

                # logging.info(
                #     f"🚑 Ambulance {ambulance_id} | travel_time: {travel_time} | route points: {len(points)}")

                if not points or len(points) < 2:
                    logging.warning(
                        f"⚠️ Not enough route points for ambulance {ambulance_id}. Skipping.")
                    continue

                ts_min = points[0]["timestamp"]
                ts_max = points[-1]["timestamp"]
                # logging.info(f"🕒 ts_min: {ts_min}, ts_max: {ts_max}")

                ambu_response = client.execute_command(
                    "GET", "ambulance", ambulance_id, "WITHFIELDS", "OBJECT"
                )

                if not ambu_response or len(ambu_response) < 2:
                    logging.warning(
                        f"❗ No ambulance data for ID {ambulance_id}")
                    return None

                geometry_obj = json.loads(ambu_response[0])
                coordinates = geometry_obj.get("coordinates", [])
                ts = coordinates[2] if len(coordinates) >= 3 else None

                # logging.info(f"📍 Current timestamp for ambulance {ambulance_id}: {ts}")

                if ts is None or ts_min == ts_max:
                    logging.warning(
                        f"⚠️ Invalid timestamp for ambulance {ambulance_id}. Skipping.")
                    continue

                past_time = ((ts - ts_min) / (ts_max - ts_min)) * travel_time

                # logging.info(f"✅ Computed past_time: {round(past_time, 2)}")

                results.append({
                    "ambulance_id": ambulance_id,
                    "past_time": round(past_time, 2),
                    "travel_time": travel_time,
                    "eta": ts_max,
                })

            except Exception as e:
                logging.warning(
                    f"⚠️ Skipped entry for object {item} due to error: {e}")

    except Exception as e:
        logging.error(f"🚨 Failed to retrieve ambulance progress: {e}")

    return results
