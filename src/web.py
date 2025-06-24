import streamlit as st
import asyncio
from utils.web_layout import configure_app, initialize_session_state
from utils.web_sidebar import render_sidebar
from utils.web_dashboard import render_dashboard

async def main():
    configure_app()
    initialize_session_state()

    with st.sidebar:
        render_sidebar()

    render_dashboard()

if __name__ == "__main__":
    asyncio.run(main())
