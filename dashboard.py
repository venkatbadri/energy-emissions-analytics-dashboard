#Import necessary packages
import streamlit as st
import pandas as pd
import pydeck as pdk
import random
import json
import paho.mqtt.client as mqtt
import threading

# -----------------------------
# Setup "MQTT SUBSCRIBE" using "mosquitto"
# -----------------------------

MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "electricity/data"

if "live_data" not in st.session_state:
    st.session_state["live_data"] = []

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker.")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        st.session_state["live_data"].append(data)
        print(f"Received live data: {data}")
    except Exception as e:
        print("Error parsing MQTT message:", e)

def mqtt_thread():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

# Start MQTT in a background thread
thread = threading.Thread(target=mqtt_thread, daemon=True)
thread.start()

# -----------------------------
# Configure the dataset which is created from "datasets_combined.py"
# -----------------------------
CSV_FILE = "full_merged_dataset.csv"

# Display columns with most data presented
ESSENTIAL_COLS = [
    "Facility name",
    "State",
    "Fuel Source",
    "MW Capacity",
    "Total emissions t CO2 e",
    "emission_intensity"
]

# -----------------------------
# Load the dataset
# -----------------------------

df = pd.read_csv(CSV_FILE, low_memory=False)

# Keep only essential columns
ESSENTIAL_COLS_EXIST = [col for col in ESSENTIAL_COLS if col in df.columns]
df = df[ESSENTIAL_COLS_EXIST].copy()

# Convert live data from "session state" to "DataFrame"
if st.session_state["live_data"]:
    live_df = pd.DataFrame(st.session_state["live_data"])
    # Merge live data (update latest power/emissions)
    if not live_df.empty and "facility_code" in df.columns:
        df = df.merge(
            live_df.groupby("facility_code").last().reset_index(),
            on="facility_code", how="left", suffixes=("", "_live")
        )
        if "power_live" in df.columns:
            df["power"] = df["power_live"].combine_first(df.get("power"))
        if "emissions_live" in df.columns:
            df["emissions"] = df["emissions_live"].combine_first(df.get("emissions"))
        df.drop(columns=[c for c in df.columns if c.endswith("_live")], inplace=True)


# Convert numeric columns safely
for col in ["MW Capacity", "Total emissions t CO2 e", "emission_intensity"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# -----------------------------
# Approximate the Lat/Log for the Australian States
# -----------------------------

state_coords = {
    "NSW": [-33.8688, 151.2093],
    "VIC": [-37.8136, 144.9631],
    "QLD": [-27.4698, 153.0251],
    "SA": [-34.9285, 138.6007],
    "WA": [-31.9505, 115.8605],
    "TAS": [-42.8821, 147.3272],
    "NT": [-12.4634, 130.8456],
    "ACT": [-35.2809, 149.1300]
}

def get_lat_lon(state):
    base = state_coords.get(state, [-25, 135])
    jitter = lambda: random.uniform(-0.5, 0.5)
    return base[0] + jitter(), base[1] + jitter()

df["lat"], df["lon"] = zip(*df["State"].map(get_lat_lon))

# -----------------------------
# Streamlit Page
# -----------------------------

st.set_page_config(page_title="Electricity Facilities Map", layout="wide")
st.title("Electricity Facilities Map & Facility Data")

# -----------------------------
# Sidebar filters & reset
# -----------------------------
st.sidebar.header("Filters")


# Search by name
search_facility = st.sidebar.text_input("Search Facility by Name")

# State filter
states = sorted(df["State"].dropna().unique())
state_options = ["All States"] + states
selected_states = st.sidebar.multiselect("Select States", state_options, default=["All States"])
if "All States" in selected_states:
    selected_states = states

# Technology filter
techs = sorted(df["Fuel Source"].dropna().unique())
tech_options = ["All Technologies"] + techs
selected_techs = st.sidebar.multiselect("Select Technology", tech_options, default=["All Technologies"])
if "All Technologies" in selected_techs:
    selected_techs = techs


# -----------------------------
# Apply the filter in the search section
# -----------------------------

# Handle "All" selections
if "All" in selected_states:
    filtered_states = states  # all states
else:
    filtered_states = selected_states

if "All" in selected_techs:
    filtered_techs = techs  # all technologies
else:
    filtered_techs = selected_techs

# Apply filters
df_filtered = df[
    (df["State"].isin(filtered_states)) &
    (df["Fuel Source"].isin(filtered_techs))
]


# Apply search filter
selected_facility = None
if search_facility:
    search_df = df_filtered[df_filtered["Facility name"].str.contains(search_facility, case=False, na=False)]
    if not search_df.empty:
        selected_facility = search_df.iloc[0]
        df_filtered = search_df

st.sidebar.write(f"Showing {len(df_filtered)} facilities")

# -----------------------------
# Append live MQTT data
# -----------------------------

if st.session_state["live_data"]:
    live_df = pd.DataFrame(st.session_state["live_data"])
    if not live_df.empty:
        df_live = df.merge(live_df, on="facility_code", how="left")
        df_filtered = pd.concat([df_filtered, df_live]).drop_duplicates(subset=["facility_code"], keep="last")


# -----------------------------
# Display the table
# -----------------------------

st.subheader("Facility Data Table")
st.dataframe(df_filtered, height=400)

# -----------------------------
# Downsample if too big
# -----------------------------

MAX_POINTS = 5000
if len(df_filtered) > MAX_POINTS:
    df_filtered = df_filtered.sample(n=MAX_POINTS, random_state=42)

# -----------------------------
# Map View
# -----------------------------

center_lat, center_lon, zoom_level = -25, 135, 4
if selected_facility is not None:
    center_lat = selected_facility["lat"]
    center_lon = selected_facility["lon"]
    zoom_level = 8

layer = pdk.Layer(
    "ScatterplotLayer",
    data=df_filtered,
    get_position='[lon, lat]',
    get_fill_color='[200, 30, 0, 160]',
    get_radius=5000,
    pickable=True
)

tooltip = {
    "html": "<b>{Facility name}</b><br>State: {State}<br>Tech: {Fuel Source}<br>Capacity: {MW Capacity} MW<br>Total Emissions: {Total emissions t CO2 e}<br>Emission Intensity: {emission_intensity}",
    "style": {"backgroundColor": "white", "color": "black", "fontSize": "14px"},
}

view_state = pdk.ViewState(
    latitude=center_lat,
    longitude=center_lon,
    zoom=zoom_level,
    pitch=0
)

r = pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip)
st.pydeck_chart(r)
