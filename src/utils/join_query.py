from streamlit_autorefresh import st_autorefresh

import json
import logging
from datetime import datetime, timedelta

import streamlit as st


def scan_broken_train_incidents(client):
    """扫描 broken_train 获取所有事件数据"""
    results = {}
    try:
        response = client.execute_command("SCAN", "broken_train")

        # logging.info(f"response: {response}")
        items = response[1] if len(response) > 1 else []

        for item in items:
            object_id = item[0] if isinstance(item, list) else item
            object_id = object_id.decode() if isinstance(
                object_id, bytes) else str(object_id)

            get_response = client.execute_command(
                "GET", "broken_train", object_id, "WITHFIELDS")
            if not get_response or len(get_response) < 2:
                continue

            fields_raw = get_response[1]
            fields = dict(zip(fields_raw[::2], fields_raw[1::2]))

            geometry = get_response[0]
            geometry_obj = json.loads(geometry) if isinstance(geometry, (str, bytes)) else geometry

            parsed = parse_incident_metadata(object_id, fields, geometry_obj)
            
            # parsed = parse_incident_metadata(object_id, fields)
            if parsed:
                # results[parsed["incident_id"]] = parsed
                results[object_id] = parsed 

    except Exception as e:
        logging.error(f"Failed to scan broken train: {e}")
    return results


def parse_incident_metadata(object_id, fields, geometry=None):
    try:
        incident_id = object_id.split("_")[1]
        ts_ms = int(geometry["coordinates"][2]) if geometry else None
        start_time = datetime.fromtimestamp(ts_ms / 1000) if ts_ms else None

        return {
            "incident_id": incident_id,
            "start_time": start_time,
            "affected_passengers": fields.get("affected_passengers"),
            "description": fields.get("description"),
            "expected_resolving_time": int(fields.get("expected_resolving_time", 0)),
            "status": fields.get("status"),
            "severity": fields.get("severity") or "low"
        }
    except Exception as e:
        logging.warning(f"Failed to parse incident {object_id}: {e}")
        return None



def group_ambulances_by_incident(progress_data):
    """将 ambulance 数据按 incident_id 分组"""
    grouped = {}
    for ambu in progress_data:
        incident_id = ambu.get("incident_train_id", "Unknown")
        grouped.setdefault(incident_id, []).append(ambu)
    return grouped


def render_incident_block(incident_id, incident_data, ambulances):
    severity = str(incident_data.get("severity")).lower()
    color_map = {
        "critical": "#ffe6e6",  # light red
        "high": "#fff2cc",      # light orange
        "moderate": "#e6f7ff",  # light blue
        "low": "#f0f0f0"        # light grey
    }
    box_color = color_map.get(severity, "#f0f0f0")

    latest_eta = max(a["eta"] for a in ambulances)
    resolution_time = datetime.fromtimestamp(latest_eta / 1000) + timedelta(
        minutes=incident_data.get("expected_resolving_time", 0)
    )

    with st.container():
        st.markdown(
            f"""
            <div style="
                background-color: {box_color};
                border-left: 6px solid #ff6f61;
                padding: 15px;
                margin-bottom: 25px;
                border-radius: 8px;
            ">
            <h4>🚨 Incident {incident_id}</h4>
            🕐 <b>Occurred at:</b> {incident_data['start_time'].strftime('%Y-%m-%d %H:%M:%S')}<br>
            👥 <b>Passengers affected:</b> {incident_data['affected_passengers']}<br>
            📋 <b>Description:</b> {incident_data['description']}<br>
            💡 <b>Severity:</b> {severity.capitalize()}<br>
            🧩 <b>Estimated Resolution Time:</b> {incident_data['expected_resolving_time']} mins<br>
            ✅ <b>Projected Resolution:</b> {resolution_time.strftime('%Y-%m-%d %H:%M:%S')}<br><br>
            """,
            unsafe_allow_html=True
        )

        for ambu in ambulances:
            percent = min(ambu["past_time"] / ambu["travel_time"], 1.0) if ambu["travel_time"] else 0.0
            remaining = max(ambu["travel_time"] - ambu["past_time"], 0)
            eta_str = datetime.fromtimestamp(ambu["eta"] / 1000).strftime("%Y-%m-%d %H:%M:%S")

            st.markdown(
                f"<b>🚑 Ambulance {ambu['ambulance_id']}</b><br>"
                f"📅 ETA: {eta_str} | ⏳ Remaining: {int(remaining // 60)}m {int(remaining % 60)}s<br>",
                unsafe_allow_html=True
            )
            st.progress(percent)

        st.markdown("</div>", unsafe_allow_html=True)



def display_rescue_progress_auto(client):
    st_autorefresh(interval=2000, key="rescue_polling")

    progress_data = get_ambulance_progress(client)
    if not progress_data:
        st.info("🚑 No active rescue in progress.")
        return

    incident_groups = group_ambulances_by_incident(progress_data)
    all_incidents = scan_broken_train_incidents(client)

    for incident_id, ambulances in incident_groups.items():
        incident_data = next(
            (data for key, data in all_incidents.items() if incident_id in key), None
        )

        

        # incident_data = all_incidents.get(incident_id)
        if incident_data:
            render_incident_block(incident_id, incident_data, ambulances)
        else:
            st.warning(f"⚠️ Incident {incident_id} not found in broken_train.")


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

                incident_id = str(object_id).split("_")[0]

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
                    "incident_train_id": incident_id
                })

            except Exception as e:
                logging.warning(
                    f"⚠️ Skipped entry for object {item} due to error: {e}")

    except Exception as e:
        logging.error(f"🚨 Failed to retrieve ambulance progress: {e}")

    return results
