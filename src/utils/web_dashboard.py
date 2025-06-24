import streamlit as st
from utils.web_map import render_map_section
from utils.web_controls import render_train_controls
from utils.web_ambulance import display_ambulance_availabitity_data
from utils.join_query import display_rescue_progress_auto

import redis


client = redis.Redis(host="tile38", port=9851, decode_responses=True)


def render_dashboard():
    col = st.columns((2.7, 4.8, 1), gap="small")

    with col[0]:
        display_ambulance_availabitity_data()

    with col[1]:
        render_map_section()

        display_rescue_progress_auto(client)



    with col[2]:
        render_train_controls()
