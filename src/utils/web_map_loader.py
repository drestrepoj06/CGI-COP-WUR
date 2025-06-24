import json
import streamlit as st

def load_map_html(filepath="animated_map.html", route_points=None):
    with open("utils/UtrechtRails.geojson") as f:
        rails = json.load(f)
    with open("utils/AmbulanceStations.geojson") as f:
        stations = json.load(f)

    route_points_js = json.dumps(route_points or [])
    html = open(filepath).read()

    html = html.replace("//__INSERT_RAILS_HERE__", f"const railsData = {json.dumps(rails)};")
    html = html.replace("//__INSERT_AMBULANCE_STATIONS_HERE__", f"const ambulanceStationsData = {json.dumps(stations)};")

    return html

def generate_incident_js():
    if not st.session_state['button_states'].get('show_incident') or not st.session_state.get("incident_data"):
        return "", ""

    incident = st.session_state["incident_data"]
    coords = incident["location"]["coordinates"]
    lng, lat = coords[0], coords[1]

    js_incident = f"""
        <script>
            showIncidentPopup(
                {lat}, {lng},
                "{incident['train_id']}",
                "{incident.get('train_type', 'Unknown')}",
                "{incident['severity'].capitalize()}",
                "{incident['description'].capitalize()}"
            );
        </script>
    """

    segments = st.session_state.get("inactive_segments", [])
    js_segments = f"""
        <script>
            const inactiveSegments = {json.dumps(segments)};
            inactiveSegments.forEach(segment => {{
                L.geoJSON(segment, {{
                    style: {{
                        color: 'red',
                        weight: 3,
                        opacity: 0.7
                    }}
                }}).addTo(map);
            }});
        </script>
    """ if segments else ""

    return js_incident, js_segments
