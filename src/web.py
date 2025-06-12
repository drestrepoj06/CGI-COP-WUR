import streamlit as st
import json
import redis
import pandas as pd
import altair as alt

import logging

# Streamlit app setup
st.set_page_config(
    page_title="COP Dashboard",
    page_icon="🚅",
    layout="wide",
    initial_sidebar_state="collapsed",)

# Tile38 setup
TILE38_HOST = 'tile38'
TILE38_PORT = 9851
tile38 = redis.Redis(host=TILE38_HOST, port=TILE38_PORT, decode_responses=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")    



def query_tile38(collection):
    """Query Tile38 for all objects in a collection."""
    try:
        response = tile38.execute_command("SCAN", collection)
        logging.info(f"Tile38 response content: {response}")  # 输出到终端进行调试
        return response
    except Exception as e:
        logging.error(f"Error querying {collection}: {e}")
        return None

@st.cache_data
def load_train_data():
    response = query_tile38("train")
    train_data = []

    if response:
        count, objects = response  # 解包元组
        for obj in objects:
            object_id = obj[0]  # 获取对象 ID
            coordinates_json = json.loads(obj[1])  # 解析坐标 JSON
            fields_json = json.loads(obj[2][1])  # 解析 fields JSON

            train_data.append({
                "timestamp": coordinates_json["coordinates"][2],  # 时间戳 (Z 值)
                "features": [{
                    "lat": coordinates_json["coordinates"][1],  # 纬度
                    "lon": coordinates_json["coordinates"][0],  # 经度
                    "ritId": object_id,  # 用 object ID 作为 ritId
                    "speed": fields_json.get("speed", 0),
                    "type": fields_json.get("type", "Unknown"),
                    "timestamp": coordinates_json["coordinates"][2]  # 时间戳
                }]
            })

    return json.dumps(train_data)

@st.cache_data
def load_ambulance_data():
    response = query_tile38("ambulance")
    ambulance_data = []

    if response:
        count, objects = response  # 解包元组
        for obj in objects:
            object_id = obj[0]  # 获取对象 ID
            coordinates_json = json.loads(obj[1])  # 解析坐标 JSON
            fields_json = json.loads(obj[2][1])  # 解析 fields JSON

            ambulance_data.append({
                "timestamp": coordinates_json["coordinates"][2],  # 时间戳 (Z 值)
                "features": [{
                    "lat": coordinates_json["coordinates"][1],  # 纬度
                    "lon": coordinates_json["coordinates"][0],  # 经度
                    "vehicle_number": fields_json.get("vehicle_number", "Unknown"),
                    "speed": fields_json.get("speed", 0),
                    "type": fields_json.get("type", "Unknown"),
                    "heading": fields_json.get("heading", 0),
                    "accuracy": fields_json.get("accuracy", 0),
                    "source": fields_json.get("source", "Unknown")
                }]
            })

    return json.dumps(ambulance_data)


# Fetch data from Tile38
train_js_data = load_train_data()
ambulance_js_data = load_ambulance_data()


# Donut chart generator
def make_pie(input_response, input_text, input_color):
    if input_color == 'blue':
        chart_color = ['#29b5e8', '#155F7A']
    elif input_color == 'green':
        chart_color = ['#27AE60', '#12783D']
    elif input_color == 'red':
        chart_color = ['#E74C3C', '#781F16']
    elif input_color == 'yellow':
        chart_color = ["#F9ED00", "#A79D09"]
    
    source = pd.DataFrame({
        "Status": ['Available', 'In Use'],
        "Value": [100 - input_response, input_response],
    })

    plot = alt.Chart(source).mark_arc().encode(
        theta="Value:Q",
        color=alt.Color("Status:N",
                        scale=alt.Scale(range=chart_color),
                        legend=alt.Legend(title="Ambulance Status")),
        tooltip=["Status:N", "Value:Q"]
    ).properties(width=130, height=130, title=input_text)
    
    return plot

# Inject into HTML
with open("animated_map.html", "r") as f:
    html = f.read()

html = html.replace("//__INSERT_TRAIN_DATA_HERE__", f"const trainData = {train_js_data};\nconst ambulanceData = {ambulance_js_data};")


# Sidebar
with st.sidebar:
    st.title('COP dashboard')

# Create columns for layout
col = st.columns((2.5, 5, 1), gap='small')

# Left column
with col[0]:
    # Ambulance data table
    st.subheader("Ambulance station availability")

    # Sample data
    ambulance_data = pd.DataFrame({
        "Station": ["Maarssen", "Vader Rijndreef", "Diakonessenhuis", "On the move"],
        "Ambulances": [3, 5, 2, 5],
        "Capacity": [5, 6, 4, 8]
    })

    ambulance_data["In Use"] = ambulance_data["Capacity"] - ambulance_data["Ambulances"]
    
    # Display the table
    st.dataframe(
        ambulance_data,
        column_order=("Station", "Ambulances"),
        hide_index=True,
        width=None,
        column_config={
            "Station": st.column_config.TextColumn(
                "Station",
                width="small"
            ),
            "Ambulances": st.column_config.ProgressColumn(
                "Available",
                format="%d",  # Display as integer
                min_value=0,
                max_value=max(ambulance_data["Capacity"]),
                width="small"
            )
        }
    )    
    
    st.subheader("Ambulance availability")
    
    colors = ['red', 'green', 'blue', 'yellow']
    
    for i, row in ambulance_data.iterrows():
        pct = int((row["In Use"] / row["Capacity"]) * 100)
        st.altair_chart(make_pie(pct, row["Station"], colors[i]), use_container_width=True)


# Middle column
with col[1]:
    st.components.v1.html(html, height=500, scrolling=False) # Show the animated map

# Right column
with col[2]:
    st.button("Button 1")
    st.button("Button 2")
    st.button("Button 3")

