import streamlit as st
import json
from utils.web_map_loader import load_map_html, generate_incident_js


def render_map_section():
    map_html = load_map_html()
    incident_js, segment_js = generate_incident_js()
    st.components.v1.html(map_html + incident_js +
                          segment_js, height=525, scrolling=False)
