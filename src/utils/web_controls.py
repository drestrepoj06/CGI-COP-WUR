import streamlit as st
from datetime import datetime
import logging
from websocket_server import mark_random_train_as_inactive, reset_all_trains
import redis
import time

from utils.navigate import clear_ambu_path_and_broken_train
from consumer import process_broken_trains_and_assign_ambulances

client = redis.Redis(host="tile38", port=9851, decode_responses=True)

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


def render_train_controls():
    st.markdown("#### Dashboard Control")
    display_stop_button()
    display_rescue_ambu()
    display_reset_button()
    display_button_succes_messages()


def display_rescue_ambu():
    if st.button("🚑 Request Ambulances", key="rescue_ambu_button"):
        try:
            result = process_broken_trains_and_assign_ambulances()
            if result["status"] == "success":
                st.session_state["tqdm"] = True
                st.rerun()
            else:
                st.error("Dispatch failed: " +
                         result.get("error", "Unknown reason"))
        except Exception as e:
            logging.error(f"Rescue dispatch failed: {e}")
            st.error("An error occurred while requesting ambulances.")

    st.caption("(This requests ambulance(s))")


def display_stop_button():
    if st.button("🛑 Simulate an incident", key="stop_train_button"):
        stop_button_action()
    st.caption("(This stops a random train)")

def stop_button_action():
    max_retries = 30
    for attempt in range(max_retries):
        try:
            st.session_state['incident_data'] = None
            st.session_state['button_states']['show_reset_success'] = False

            incident = mark_random_train_as_inactive(client)
            if incident:
                st.session_state['incident_data'] = incident.get("incident")
                st.session_state['inactive_segments'] = incident.get(
                    "inactive_segments", [])
                st.session_state['button_states']['show_incident'] = True

            st.rerun()
        except Exception as e:
            logging.error(f"Failed to simulate incident: {e}")
            st.error("An error occurred while simulating an incident.")

def display_reset_button():
    if st.button("🔄 Resolve the incident", key="reset_train_button"):
        try:
            reset_all_trains(client)
            clear_ambu_path_and_broken_train(client)

            st.session_state['button_states']['show_incident'] = False
            st.session_state['button_states']['show_reset_success'] = True
            st.session_state["tqdm"] = False

            st.rerun()
        except Exception as e:
            logging.error(f"Failed to reset trains: {e}")
            st.error("An error occurred while resetting trains.")

    st.caption("(This resets all trains and ambulances)")


def display_button_succes_messages():
    if st.session_state['button_states'].get('show_incident') and st.session_state.get('incident_data'):
        st.success("An incident was simulated!")

    if st.session_state['button_states'].get('show_reset_success'):
        st.success("All trains have been reset to active status!")


def display_incident_summary():
    if st.session_state['button_states'].get('show_incident') and st.session_state.get('incident_data'):
        incident = st.session_state['incident_data']
        coords = incident.get("location", {}).get("coordinates", [0, 0, 0])
        lng, lat, timestamp = coords[0], coords[1], int(
            coords[2]) if len(coords) > 2 else 0
        readable_time = datetime.utcfromtimestamp(
            timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

        st.markdown(f"""
        ##### 🚨 Incident occured!
        - **Train ID**: {incident.get('train_id')}
        - **Location**: {lat}, {lng}  
        - **Timestamp**: {readable_time} UTC  
        - **Passengers Affected**: {incident.get('affected_passengers')}  
        - **Ambulance Units Required**: {incident.get('ambulance_units')}  
        - **Technical Resources Required**: {incident.get('technical_resources', 'N/A').capitalize()}
        """)


def display_ambulance_alerts():
    st.markdown("#### Ambulances")

    try:
        alerts = redis_client.lrange("ambulance_alerts", 0, 19)

        if alerts:
            # Only show count if there are alerts
            ambu_inside_count = redis_client.scard("ambulance_inside_once")
            st.info(f"🟨 Ambulances inside geofence: **{ambu_inside_count}**")

            for msg in alerts:
                decoded_msg = msg.decode() if isinstance(msg, bytes) else msg
                st.warning(f"🚑 {decoded_msg} 🚨")
        else:
            st.success("No current ambulance alerts.")
    except Exception as e:
        logging.error(f"Error loading ambulance alerts: {e}")
        st.error("Could not load ambulance alerts.")


def display_train_alerts():
    st.markdown("#### Trains")

    try:
        alerts = redis_client.lrange("train_alerts", 0, 19)

        if alerts:
            # Only show count if there are alerts
            train_inside_count = redis_client.scard("train_inside_once")
            st.info(f"🟦 Trains inside geofence: **{train_inside_count}**")

            for msg in alerts:
                decoded_msg = msg.decode() if isinstance(msg, bytes) else msg
                st.info(f"🚆 {decoded_msg} 🚨")
        else:
            st.success("No train alerts.")
    except Exception as e:
        logging.error(f"Failed to fetch train alerts: {e}")
        st.error("Could not load train alerts.")
