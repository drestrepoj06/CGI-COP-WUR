import streamlit as st
from utils.web_map import render_map_section
from utils.web_controls import render_train_controls, display_incident_summary
from utils.web_ambulance import display_ambulance_availabitity_data
from utils.join_query import display_rescue_progress_auto

from utils.web_controls import display_ambulance_alerts, display_train_alerts

import redis


client = redis.Redis(host="tile38", port=9851, decode_responses=True)


def render_dashboard():
    col = st.columns((2, 5.5, 1), gap="small")

    with col[0]:
        st.markdown("#### Incident Summary")
        display_incident_summary()
        display_rescue_progress_auto(client)

    with col[1]:
        render_map_section()
        display_ambulance_availabitity_data()

    with col[2]:
        render_train_controls()

        # Button to manually refresh the alerts
        if st.button("Refresh Alerts"):
            display_ambulance_alerts()
            display_train_alerts()
