import streamlit as st
from datetime import datetime
import logging
from websocket_server import mark_random_train_as_inactive, reset_all_trains
import redis

from utils.navigate import clear_ambu_path_and_broken_train


client = redis.Redis(host="tile38", port=9851, decode_responses=True)


def render_train_controls():
    st.markdown("### Train Control")
    display_stop_button()
    display_reset_button()
    st.markdown("---")
    display_incident_summary()


def display_stop_button():
    if st.button(
        "🛑 Simulate an incident",
        disabled=st.session_state['button_states']['stop_disabled'],
        key="stop_train_button"
    ):
        try:
            st.session_state['incident_data'] = None
            st.session_state['button_states']['stop_disabled'] = True
            st.session_state['button_states']['reset_disabled'] = False
            st.session_state['button_states']['show_reset_success'] = False

            incident = mark_random_train_as_inactive(client)
            if incident:
                st.session_state['incident_data'] = incident.get("incident")
                st.session_state['inactive_segments'] = incident.get("inactive_segments", [])
                st.session_state['button_states']['show_incident'] = True
            st.rerun()

        except Exception as e:
            logging.error(f"Failed to simulate incident: {e}")
            st.error("An error occurred while simulating an incident.")

    st.caption("(This stops a random train)")


def display_reset_button():
    if st.button(
        "🔄 Resolve the incident",
        disabled=st.session_state['button_states']['reset_disabled'],
        key="reset_train_button"
    ):
        try:
            reset_all_trains(client)

            clear_ambu_path_and_broken_train(client)

            st.session_state['button_states']['stop_disabled'] = False
            st.session_state['button_states']['reset_disabled'] = True
            st.session_state['button_states']['show_incident'] = False
            st.session_state['button_states']['show_reset_success'] = True
            st.rerun()

        except Exception as e:
            logging.error(f"Failed to reset trains: {e}")
            st.error("An error occurred while resetting trains.")

    st.caption("(This will reset all trains)")


def display_incident_summary():
    if st.session_state['button_states'].get('show_incident') and st.session_state.get('incident_data'):
        incident = st.session_state['incident_data']
        coords = incident.get("location", {}).get("coordinates", [0, 0, 0])
        lng, lat, timestamp = coords[0], coords[1], int(coords[2]) if len(coords) > 2 else 0
        readable_time = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

        st.success("An incident was simulated!")

        st.markdown(f"""
        ### 🚨 Incident Summary  
        - **Train ID**: {incident.get('train_id')}
        - **Train Type**: {incident.get('train_type', 'Unknown')}
        - **Location**: {lat}, {lng}  
        - **Timestamp**: {readable_time} UTC  
        - **Passengers Affected**: {incident.get('affected_passengers')}  
        - **Ambulance Units Required**: {incident.get('ambulance_units')}  
        - **Technical Resources Required**: {incident.get('technical_resources', 'N/A').capitalize()}
        """)

    if st.session_state['button_states'].get('show_reset_success'):
        st.success("All trains have been reset to active status!")
