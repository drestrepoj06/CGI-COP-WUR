import streamlit as st
import time
from utils.web_map import render_map_section
from utils.web_controls import render_train_controls, display_ambulance_alerts, display_train_alerts
from utils.web_ambulance import display_ambulance_data

def render_dashboard():

    # Create columns for layout
    col = st.columns((2.7, 4.8, 1), gap="small")

    with col[0]:
        # Render the ambulance data and control buttons
        ambulance_data = display_ambulance_data()

        # Button to manually refresh the alerts
        if st.button("Refresh Alerts"):
            display_ambulance_alerts()
            display_train_alerts()

    with col[1]:
        render_map_section()

    with col[2]:
        render_train_controls()
