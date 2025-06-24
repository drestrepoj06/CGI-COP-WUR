import streamlit as st
import pandas as pd
import altair as alt

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


