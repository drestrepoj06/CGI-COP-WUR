from plotly import graph_objects as go
import streamlit as st
import pandas as pd
import altair as alt
import logging

# Streamlit app setup
st.set_page_config(
    page_title="RCOP Dashboard",
    page_icon="🚅",
    layout="wide",
    initial_sidebar_state="collapsed",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Function to create a pie (donut) chart
def make_pie_chart(usage_percentage, station_name, color_scheme):
    """
    Generates a donut chart displaying ambulance availability status.

    Parameters:
    - usage_percentage (int): Percentage of ambulances in use.
    - station_name (str): Name of the station.
    - color_scheme (str): Color scheme for the chart.

    Returns:
    - alt.Chart object: Donut chart representation.
    """
    colors = {
        'blue': ['#29b5e8', '#155F7A'],
        'green': ['#27AE60', '#12783D'],
        'red': ['#E74C3C', '#781F16'],
        'yellow': ["#F9ED00", "#A79D09"]
    }

    source = pd.DataFrame({
        "Status": ['Available', 'In Use'],
        "Value": [100 - usage_percentage, usage_percentage],
    })

    return alt.Chart(source).mark_arc().encode(
        theta="Value:Q",
        color=alt.Color("Status:N", scale=alt.Scale(range=colors[color_scheme]), legend=alt.Legend(title="Ambulance Status")),
        tooltip=["Status:N", "Value:Q"]
    ).properties(width=130, height=130, title=station_name)

# Function to load animated map HTML
def load_map_html(filepath="animated_map.html"):
    """
    Loads an animated map from an HTML file.

    Parameters:
    - filepath (str): Path to the HTML file.

    Returns:
    - str: HTML content of the map.
    """
    with open(filepath, "r") as f:
        return f.read()

# Function to create and display the ambulance data table
def display_ambulance_data():
    """
    Displays the ambulance availability table in Streamlit.
    """
    st.subheader("Ambulance station availability")

    # Sample data representing station capacities and availability
    ambulance_data = pd.DataFrame({
        "Station": ["Maarssen", "Vader Rijndreef", "Diakonessenhuis", "On the move"],
        "Ambulances": [3, 5, 2, 5],
        "Capacity": [5, 6, 4, 8]
    })

    ambulance_data["In Use"] = ambulance_data["Capacity"] - ambulance_data["Ambulances"]

    # Display the table with customized columns
    st.dataframe(
        ambulance_data,
        column_order=("Station", "Ambulances"),
        hide_index=True,
        column_config={
            "Station": st.column_config.TextColumn("Station", width="small"),
            "Ambulances": st.column_config.ProgressColumn("Available", format="%d", min_value=0, max_value=max(ambulance_data["Capacity"]), width="small")
        }
    )

    return ambulance_data

# Function to display ambulance availability charts
def display_availability_charts(ambulance_data):
    """
    Generates and displays donut charts for ambulance availability.

    Parameters:
    - ambulance_data (DataFrame): Data containing ambulance usage stats.
    """
    st.subheader("Ambulance availability")

    color_options = ['red', 'green', 'blue', 'yellow']

    for i, row in ambulance_data.iterrows():
        usage_percentage = int((row["In Use"] / row["Capacity"]) * 100)
        st.altair_chart(make_pie_chart(usage_percentage, row["Station"], color_options[i]), use_container_width=True)

def display_ambulance_progress_bars():
    """
    Displays progress bars for Onroute and Arrived ambulances vs total needed.
    """
    st.subheader("Ambulance Response Progress")

    # Made-up data
    total_needed = 10
    onroute = 6
    arrived = 3

    st.write(f"**Onroute Ambulances:** {onroute} / {total_needed}")
    st.progress(onroute / total_needed)

    st.write(f"**Arrived Ambulances:** {arrived} / {total_needed}")
    st.progress(arrived / total_needed)

def display_ambulance_funnel():
    """
    Displays a funnel chart for ambulance response stages.
    """
    # Made-up data
    total_needed = 2
    sent_to_site = 1
    arrived = 1

    fig = go.Figure(go.Funnel(
        y = [
            "Total Needed",
            "Ambulances Sent",
            "Ambulances Arrived"
        ],
        x = [
            total_needed,
            sent_to_site,
            arrived
        ],
        textposition = "inside",
        textinfo = "value+percent initial",
        opacity = 0.85,
        marker = {
            "color": ["#781F16", "#A79D09", "#12783D"],  # dark red, dark yellow, dark green
            "line": {
                "width": [3, 3, 3],
                "color": ["#781F16", "#A79D09", "#12783D"]
            }
        },
        connector = {
            "line": {
                "color": "gray",
                "dash": "dot",
                "width": 2
            }
        }
    ))

    fig.update_layout(
        title="Ambulance Response Funnel",
        margin=dict(l=10, r=10, t=40, b=10),
        height=350
    )

    st.plotly_chart(fig, use_container_width=True)

def display_ambulance_stacked_bar():
    """
    Displays a Likert-style horizontal stacked bar for ambulance progress.
    """
    import plotly.graph_objects as go

    # Example data
    total_needed = 10
    sent = 6
    arrived = 3
    not_sent = total_needed - sent

    # Each segment: [Arrived, Sent but not arrived, Not yet sent]
    x_data = [[arrived, sent - arrived, not_sent]]
    y_data = ['Ambulance Dispatch Progress']

    colors = [
        'rgba(18,120,61,0.85)',   # dark green (Arrived)
        'rgba(167,157,9,0.85)',   # dark yellow (Sent, not arrived)
        'rgba(120,31,22,0.85)'    # dark red (Not yet sent)
    ]
    labels = ['Arrived', 'Sent (En Route)', 'Not Sent']

    fig = go.Figure()

    for i in range(len(x_data[0])):
        for xd, yd in zip(x_data, y_data):
            fig.add_trace(go.Bar(
                x=[xd[i]], y=[yd],
                orientation='h',
                marker=dict(
                    color=colors[i],
                    line=dict(color='rgb(248, 248, 249)', width=1)
                ),
                name=labels[i],
                hovertemplate=f"{labels[i]}: %{{x}}<extra></extra>"
            ))

    fig.update_layout(
        xaxis=dict(
            showgrid=False,
            showline=False,
            showticklabels=False,
            zeroline=False,
            range=[0, total_needed]
        ),
        yaxis=dict(
            showgrid=False,
            showline=False,
            showticklabels=False,
            zeroline=False,
        ),
        barmode='stack',        
        margin=dict(l=120, r=10, t=60, b=40),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    # Add annotations for each segment
    annotations = []
    space = 0
    for i, val in enumerate(x_data[0]):
        if val > 0:
            annotations.append(dict(
                x=space + val / 2,
                y=y_data[0],
                text=f"{val}",
                font=dict(family='Arial', size=16, color='white'),
                showarrow=False
            ))
        space += val

    fig.update_layout(
        height=150,
        annotations=annotations
    )

    st.plotly_chart(fig, use_container_width=True)


# Main dashboard layout setup
def main():
    """
    Initializes and renders the dashboard with sidebar, tables, and charts.
    """
    # Sidebar with dashboard title
    with st.sidebar:
        st.title("RCOP Dashboard")

    # Define layout columns
    col = st.columns((2.5, 5, 1), gap="small")

    # Display ambulance data table in the left column
    with col[0]:
        display_ambulance_funnel()
        display_ambulance_progress_bars()
        ambulance_data = display_ambulance_data()
        display_ambulance_stacked_bar()
        

    # Display animated map in the middle column
    with col[1]:
        st.components.v1.html(load_map_html(), height=500, scrolling=False)

        col_donut = st.columns((1, 1, 1, 1), gap="small")
        with col_donut[0]:
            st.altair_chart(make_pie_chart(50, "Maarssen", "blue"), use_container_width=True)
        with col_donut[1]:
            st.altair_chart(make_pie_chart(30, "Vader Rijndreef", "green"), use_container_width=True)
        with col_donut[2]:
            st.altair_chart(make_pie_chart(20, "Diakonessenhuis", "red"), use_container_width=True)
        with col_donut[3]:
            st.altair_chart(make_pie_chart(10, "On the move", "yellow"), use_container_width=True)

# Run the dashboard
if __name__ == "__main__":
    main()
