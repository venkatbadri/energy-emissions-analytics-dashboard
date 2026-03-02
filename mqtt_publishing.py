#Import necessary packages
import pandas as pd
import json
import time
import paho.mqtt.client as mqtt

# -----------------------------
# Configuration for the mqtt using "mosquitto"
# -----------------------------

MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "electricity/data"

# -----------------------------
# Load the csv from the data_integration.py
# -----------------------------

csv_path = "consolidated_data.csv"

try:
    df = pd.read_csv(csv_path)
    print(f"Loaded {df.shape[0]} rows from {csv_path}")
except FileNotFoundError:
    raise SystemExit(f"File not found: {csv_path}. Run the data processing script first.")

# -----------------------------
# Publish the MQTT
# -----------------------------

def publish_mqtt(df):
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()  # asynchronous network loop
    print(f"Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Publishing to topic: '{MQTT_TOPIC}'")

    for i, row in df.iterrows():
        msg = {
            "facility_code": row.get("facility_code"),
            "timestamp": row.get("timestamp"),
            "power": row.get("power"),
            "emissions": row.get("emissions")
        }
        client.publish(MQTT_TOPIC, json.dumps(msg))

        # Create a short delay, so that Streamlit can receive updates
        time.sleep(0.1)

        if i % 50 == 0:  # printing for every 50 messages
            print(f"Sent {i} messages...")

    client.loop_stop()
    client.disconnect()  # disconnect cleanly
    print("All messages published successfully.")


if __name__ == "__main__":
    print("Starting new data retrieval & publishing cycle")
    publish_mqtt(df)
    print("Cycle complete. Waiting 60 seconds for next cycle...")
    time.sleep(60)
