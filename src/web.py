import streamlit as st
import json
import pandas as pd
import altair as alt
import logging

# Streamlit app setup
st.set_page_config(
    page_title="COP Dashboard",
    page_icon="🚅",
    layout="wide",
    initial_sidebar_state="collapsed",)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")    

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


