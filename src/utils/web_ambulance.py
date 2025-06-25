import streamlit as st
import pandas as pd
import altair as alt
import redis
import json
from streamlit_autorefresh import st_autorefresh

tile38 = redis.Redis(host="tile38", port=9851, decode_responses=True)

def fetch_ambulance_data_from_tile38():
    try:
        cursor, items = tile38.execute_command("SCAN", "ambulance")
        # st.write("Raw Redis items:", items)
        ambulances = []

        for item in items:
            try:
                # Example: ["1", geojson_string, ["info", "{...}"]]
                if not isinstance(item, list) or len(item) < 3:
                    continue

                obj_id = item[0]
                fields = item[2]

                if isinstance(fields, list) and len(fields) >= 2 and fields[0] == "info":
                    info = json.loads(fields[1])
                    station = info.get("station_name", "On the move")
                    is_available = info.get("availability_status", True) and info.get("status", True)

                    ambulances.append({
                        "id": obj_id,
                        "station": station,
                        "available": is_available
                    })

            except Exception as e:
                st.error(f"Failed to parse ambulance object: {e}")

        if not ambulances:
            st.warning("No ambulances parsed from Redis entries.")

        return ambulances

    except Exception as e:
        st.error(f"Redis error: {e}")
        return []

def display_ambulance_availabitity_data():
    # Auto-refresh
    st_autorefresh(interval=1000, limit=100, key="ambulance_refresh")

    graph_col = st.columns((3.5, 1.5), gap="small")
    with graph_col[0]:
        st.markdown("#### Ambulance station availability")

        ambulance_entries = fetch_ambulance_data_from_tile38()
        if not ambulance_entries:
            st.warning("No live ambulance data available.")
            return

        # Create DataFrame with custom sort order
        df = pd.DataFrame(ambulance_entries)
        custom_order = ["Diakonessenhuis", "Maarssen", "Vader Rijndreef", "On the move"]
        df['station'] = pd.Categorical(df['station'], categories=custom_order, ordered=True)

        # Aggregate and sort
        grouped = (df
            .groupby("station", observed=True)
            .agg(
                Available=("available", lambda x: int(sum(x))),
                Total=("available", "count")
            )
            .sort_index()
            .reset_index()
        )
        grouped["In Use"] = grouped["Total"] - grouped["Available"]

        # Calculate global max for progress bars
        global_max = int(grouped["Available"].max()) if not grouped.empty else 1

        # Create display DataFrame with all columns
        display_df = grouped.rename(columns={
            "station": "Station",
            "Available": "Available"
        })[["Station", "Available", "Total", "In Use"]].copy()

        # Display table with all columns
        st.dataframe(
            display_df,
            column_order=("Station", "Available", "Total", "In Use"),
            hide_index=True,
            column_config={
                "Station": st.column_config.TextColumn("Station", width="small"),
                "Available": st.column_config.ProgressColumn(
                    "Available",
                    format="%d",
                    min_value=0,
                    max_value=global_max,
                    width="medium"
                ),
                "Total": st.column_config.NumberColumn(
                    "Total",
                    format="%d",
                    width="25px"
                ),
                "In Use": st.column_config.NumberColumn(
                    "In Use",
                    format="%d",
                    width="25px"
                )
            }
        )
    with graph_col[1]:
        # Pie chart
        st.markdown("#### Total ambulance (un)availability")
        total_available = grouped["Available"].sum()
        total_in_use = grouped["In Use"].sum()
        st.altair_chart(make_pie_chart(total_available, total_in_use, " ", "red"))

def make_pie_chart(available, in_use, station, color_scheme):
    palette = {
        'red': ['#E74C3C', '#781F16'],
    }
    chart_data = pd.DataFrame({
        "Status": ['Available', 'In Use'],
        "Value": [available, in_use],
    })

    return alt.Chart(chart_data).mark_arc().encode(
        theta="Value:Q",
        color=alt.Color("Status:N", scale=alt.Scale(range=palette[color_scheme]),
                        legend=alt.Legend(title="Ambulance Status")),
        tooltip=[alt.Tooltip("Status:N"), alt.Tooltip("Value:Q")] 
    ).properties(width=175, height=175, title=station)
