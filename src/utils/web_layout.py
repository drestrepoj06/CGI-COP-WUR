import streamlit as st
import logging

def configure_app():
    st.set_page_config(
        page_title="RCOP Meldkamer Spoor",
        page_icon="🚅",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

def initialize_session_state():
    # Initialize button states
    if 'button_states' not in st.session_state:
        st.session_state['button_states'] = {
            'stop_disabled': False,
            'reset_disabled': True,
            'show_incident': False,
            'show_reset_success': False
        }
    
