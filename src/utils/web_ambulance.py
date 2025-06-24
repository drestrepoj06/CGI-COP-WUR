import streamlit as st
import pandas as pd
import altair as alt
import redis
import logging

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

def display_ambulance_data():
    st.subheader("Ambulance station availability")

    data = pd.DataFrame({
        "Station": ["Maarssen", "Vader Rijndreef", "Diakonessenhuis", "On the move"],
        "Ambulances": [3, 5, 2, 5],
        "Capacity": [5, 6, 4, 8]
    })

    data["In Use"] = data["Capacity"] - data["Ambulances"]

    st.dataframe(
        data,
        column_order=("Station", "Ambulances"),
        hide_index=True,
        column_config={
            "Station": st.column_config.TextColumn("Station", width="small"),
            "Ambulances": st.column_config.ProgressColumn("Available", format="%d", min_value=0, max_value=max(data["Capacity"]), width="small")
        }
    )

    return data

def display_availability_charts(data):
    st.subheader("Ambulance availability")
    colors = ['red', 'green', 'blue', 'yellow']
    cols = st.columns(4)

    for i, row in data.iterrows():
        pct = int((row["In Use"] / row["Capacity"]) * 100)
        with cols[i]:
            st.altair_chart(make_pie_chart(pct, row["Station"], colors[i]))

def make_pie_chart(percentage, station, color_scheme):
    palette = {
        'blue': ['#29b5e8', '#155F7A'],
        'green': ['#27AE60', '#12783D'],
        'red': ['#E74C3C', '#781F16'],
        'yellow': ["#F9ED00", "#A79D09"]
    }
    chart_data = pd.DataFrame({
        "Status": ['Available', 'In Use'],
        "Value": [100 - percentage, percentage],
    })

    return alt.Chart(chart_data).mark_arc().encode(
        theta="Value:Q",
        color=alt.Color("Status:N", scale=alt.Scale(range=palette[color_scheme]),
                        legend=alt.Legend(title="Ambulance Status")),
        tooltip=["Status:N", "Value:Q"]
    ).properties(width=130, height=130, title=station)


