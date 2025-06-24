# import streamlit as st
# import pandas as pd
# import altair as alt
# import logging
# import redis
# from datetime import datetime
# import asyncio
# import json

# from websocket_server import mark_random_train_as_inactive, reset_all_trains
# from utils.join_query import record_ambulance_path

# st.set_page_config(
#     page_title="RCOP Meldkamer Spoor",
#     page_icon="🚅",
#     layout="wide",
#     initial_sidebar_state="collapsed",
# )

# logging.basicConfig(level=logging.INFO,
#                     format="%(asctime)s %(levelname)s: %(message)s")


# def make_pie_chart(usage_percentage, station_name, color_scheme):
#     colors = {
#         'blue': ['#29b5e8', '#155F7A'],
#         'green': ['#27AE60', '#12783D'],
#         'red': ['#E74C3C', '#781F16'],
#         'yellow': ["#F9ED00", "#A79D09"]
#     }

#     source = pd.DataFrame({
#         "Status": ['Available', 'In Use'],
#         "Value": [100 - usage_percentage, usage_percentage],
#     })

#     return alt.Chart(source).mark_arc().encode(
#         theta="Value:Q",
#         color=alt.Color("Status:N", scale=alt.Scale(range=colors[color_scheme]),
#                         legend=alt.Legend(title="Ambulance Status")),
#         tooltip=["Status:N", "Value:Q"]
#     ).properties(width=130, height=130, title=station_name)


# def load_map_html(filepath="animated_map.html", route_points=None):
#     with open("utils/UtrechtRails.geojson", "r") as f:
#         rails_geojson = json.load(f)
#     rails_js = json.dumps(rails_geojson)

#     with open("utils/AmbulanceStations.geojson", "r") as f:
#         ambulance_stations_geojson = json.load(f)
#     ambulance_stations_js = json.dumps(ambulance_stations_geojson)

#     route_points_js = json.dumps(route_points)

#     with open(filepath, "r") as f:
#         html = f.read()
#         html = html.replace("//__INSERT_RAILS_HERE__",
#                             f"const railsData = {rails_js};")
#         html = html.replace("//__INSERT_AMBULANCE_STATIONS_HERE__",
#                             f"const ambulanceStationsData = {ambulance_stations_js};")
#         html = html.replace("//__INSERT_ROUTE_POINTS_HERE__",
#                             f"const routePoints = {route_points_js};")

#     return html


# def display_ambulance_data():
#     st.subheader("Ambulance station availability")

#     # Sample data representing station capacities and availability
#     ambulance_data = pd.DataFrame({
#         "Station": ["Maarssen", "Vader Rijndreef", "Diakonessenhuis", "On the move"],
#         "Ambulances": [3, 5, 2, 5],
#         "Capacity": [5, 6, 4, 8]
#     })

#     ambulance_data["In Use"] = ambulance_data["Capacity"] - \
#         ambulance_data["Ambulances"]

#     st.dataframe(
#         ambulance_data,
#         column_order=("Station", "Ambulances"),
#         hide_index=True,
#         column_config={
#             "Station": st.column_config.TextColumn("Station", width="small"),
#             "Ambulances": st.column_config.ProgressColumn("Available", format="%d", min_value=0, max_value=max(ambulance_data["Capacity"]), width="small")
#         }
#     )

#     return ambulance_data


# def display_availability_charts(ambulance_data):
#     st.subheader("Ambulance availability")

#     color_options = ['red', 'green', 'blue', 'yellow']

#     cols = st.columns(4)

#     for i, row in ambulance_data.iterrows():
#         usage_percentage = int((row["In Use"] / row["Capacity"]) * 100)
#         with cols[i]:
#             st.altair_chart(make_pie_chart(usage_percentage,
#                             row["Station"], color_options[i]))


# client = redis.Redis(host="tile38", port=9851, decode_responses=True)


# def stop_random_train():
#     try:
#         result = mark_random_train_as_inactive(client)
#         logging.info("✅ Marked one random train as inactive!")
#         return result
#     except Exception as e:
#         logging.error(f"[ERROR] mark_random_train_as_inactive() failed: {e}")
#         return False


# async def main():
#     with st.sidebar:
#         st.title("RCOP Meldkamer Spoor")

#     # Initialize session states
#     if 'button_states' not in st.session_state:
#         st.session_state['button_states'] = {
#             'stop_disabled': False,
#             'reset_disabled': True,
#             'show_incident': False,
#             'show_reset_success': False
#         }

#     col = st.columns((2.5, 5, 1), gap="small")

#     with col[0]:
#         ambulance_data = display_ambulance_data()

#     with col[1]:

#         map_html = load_map_html()
#         incident_js = ""
#         segment_js = ""

#         if st.session_state['button_states']['show_incident'] and st.session_state.get('incident_data'):
#             coords = st.session_state['incident_data']["location"].get(
#                 "coordinates", [])
#             lng, lat = coords[0], coords[1]
#             timestamp = int(coords[2]) if len(coords) > 2 else None

#             clean_incident = {
#                 "train_id": st.session_state['incident_data']["train_id"],
#                 "train_type": st.session_state['incident_data'].get("train_type", "Unknown"),
#                 "severity": st.session_state['incident_data']["severity"].capitalize(),
#                 "description": st.session_state['incident_data']["description"].capitalize(),
#                 "lat": lat,
#                 "lng": lng,
#                 "timestamp": timestamp
#             }

#             incident_js = f"""
#             <script>
#                 showIncidentPopup(
#                     {clean_incident['lat']},
#                     {clean_incident['lng']},
#                     "{clean_incident['train_id']}",
#                     "{clean_incident['train_type']}",
#                     "{clean_incident['severity']}",
#                     "{clean_incident['description']}"
#                 );
#             </script>
#             """

#             inactive_segments = st.session_state.get("inactive_segments", [])
#             if inactive_segments:
#                 js_segments = json.dumps(inactive_segments)
#                 segment_js = f"""
#                 <script>
#                     const inactiveSegments = {js_segments};
#                     inactiveSegments.forEach(segment => {{
#                         L.geoJSON(segment, {{
#                             style: {{
#                                 color: 'red',
#                                 weight: 3,
#                                 opacity: 0.7
#                             }}
#                         }}).addTo(map);
#                     }});
#                 </script>
#                 """

#         st.components.v1.html(map_html + incident_js +
#                               segment_js, height=500, scrolling=False)
#         display_availability_charts(ambulance_data)

#     with col[2]:
#         st.markdown("### Train Control")

#         # Button to stop a random train (simulate incident)
#         if st.button(
#             "🛑 Simulate an incident",
#             disabled=st.session_state['button_states']['stop_disabled'],
#             key="stop_train_button"
#         ):
#             st.session_state['incident_data'] = None
#             st.session_state['button_states']['stop_disabled'] = True
#             st.session_state['button_states']['reset_disabled'] = False
#             st.session_state['button_states']['show_reset_success'] = False
#             incident = stop_random_train()
#             if incident:
#                 st.session_state['incident_data'] = incident["incident"]
#                 st.session_state['inactive_segments'] = incident.get(
#                     "inactive_segments", [])
#                 st.session_state['button_states']['show_incident'] = True
#             st.rerun()
#         st.caption("(This stops a random train)")

#         # Button to reset all trains
#         if st.button(
#             "🔄 Resolve the incident",
#             disabled=st.session_state['button_states']['reset_disabled'],
#             key="reset_train_button"
#         ):
#             reset_all_trains(client)
#             st.session_state['button_states']['stop_disabled'] = False
#             st.session_state['button_states']['reset_disabled'] = True
#             st.session_state['button_states']['show_incident'] = False
#             st.session_state['button_states']['show_reset_success'] = True
#             st.rerun()
#         st.caption("(This will reset all trains)")

#         st.markdown("---")
#         if st.session_state['button_states']['show_incident'] and st.session_state.get('incident_data'):
#             st.success("An incident was simulated!")
#             coords = st.session_state['incident_data']["location"].get(
#                 "coordinates", [])
#             lng, lat = coords[0], coords[1]
#             timestamp = int(coords[2]) if len(coords) > 2 else None

#             clean_incident = {
#                 "train_id": st.session_state['incident_data']["train_id"],
#                 "train_type": st.session_state['incident_data'].get("train_type", "Unknown"),
#                 "lat": lat,
#                 "lng": lng,
#                 "timestamp": timestamp,
#                 "affected_passengers": (st.session_state['incident_data']["affected_passengers"]),
#                 "ambulance_units": (st.session_state['incident_data']["ambulance_units"]),
#                 "technical_resources": (st.session_state['incident_data']["technical_resources"]).capitalize()
#             }

#             ts = clean_incident["timestamp"]
#             readable_time = datetime.utcfromtimestamp(
#                 ts / 1000).strftime('%Y-%m-%d %H:%M:%S')

#             st.markdown(f"""
#             ### 🚨 Incident Summary  
#             - **Train ID**: {clean_incident['train_id']}
#             - **Train Type**: {clean_incident['train_type']}
#             - **Location**: {clean_incident['lat']}, {clean_incident['lng']}  
#             - **Timestamp**: {readable_time} UTC  
#             - **Passengers Affected**: {clean_incident['affected_passengers']}  
#             - **Ambulance Units Required**: {clean_incident['ambulance_units']}  
#             - **Technical Resources Required**: {clean_incident['technical_resources']}
#             """)

#         if st.session_state['button_states']['show_reset_success']:
#             st.success("All trains have been reset to active status!")

# if __name__ == "__main__":
#     asyncio.run(main())




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
