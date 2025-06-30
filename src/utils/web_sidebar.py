import streamlit as st

def render_sidebar():
    st.title("RCOP Railway Operations Center for Meldkamer Spoor")

    st.markdown("""
    🚨 **Emergency Simulation Dashboard**

    This system allows you to simulate railway incidents and visualize the coordination of emergency response units.

    Use the control panel on the right to simulate or resolve incidents in real time.
    """)

    st.markdown("---")

    st.markdown("""
    🔄 Data refreshes automatically with each action.  
    🗺️ GeoJSON maps are used for real-time geospatial rendering.
    """)
