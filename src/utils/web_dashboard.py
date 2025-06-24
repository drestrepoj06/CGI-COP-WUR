import streamlit as st
from utils.web_map import render_map_section
from utils.web_controls import render_train_controls
from utils.web_ambulance import display_ambulance_data
from utils.join_query import display_rescue_progress_auto

import redis


client = redis.Redis(host="tile38", port=9851, decode_responses=True)

def render_dashboard():
    col = st.columns((2.5, 5, 1), gap="small")

    with col[0]:
        ambulance_data = display_ambulance_data()

    with col[1]:
        render_map_section(ambulance_data)

        # display_rescue_progress_auto(client)

    with col[2]:
        render_train_controls()
