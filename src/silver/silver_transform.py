import os
import pandas as pd
from datetime import datetime

BRONZE_FILE = "/home/vignesh/smart_home_iot_pipeline/data/bronze/bronze_events.parquet"
SILVER_FOLDER = "/home/vignesh/smart_home_iot_pipeline/data/silver/"

os.makedirs(SILVER_FOLDER, exist_ok=True)

def load_bronze():
    if not os.path.exists(BRONZE_FILE):
        print("No bronze file found.")
        return None
    return pd.read_parquet(BRONZE_FILE)

def clean_motion(df):
    df = df[df["sensor_type"] == "motion"].copy()
    df["motion_detected"] = df["motion_detected"].astype(bool)
    df["confidence"] = df["confidence"].astype(float)
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    return df

def clean_temp_humidity(df):
    df = df[df["sensor_type"] == "temperature_humidity"].copy()
    df["temperature_c"] = df["temperature_c"].astype(float)
    df["humidity_percent"] = df["humidity_percent"].astype(float)
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    return df

def clean_smart_plug(df):
    df = df[df["sensor_type"] == "smart_plug"].copy()
    df["watt_hours"] = df["watt_hours"].astype(float)
    df["voltage"] = df["voltage"].astype(float)
    df["current_amps"] = df["current_amps"].astype(float)
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    return df

def clean_smoke_co(df):
    df = df[df["sensor_type"] == "smoke_co"].copy()
    df["smoke_level"] = df["smoke_level"].astype(int)
    df["co_level"] = df["co_level"].astype(int)
    df["battery_level"] = df["battery_level"].astype(int)
    df["event_ts"] = pd.to_datetime(df["event_ts"])
    return df

def save_silver(df, name):
    output_path = os.path.join(SILVER_FOLDER, f"{name}.parquet")
    df.to_parquet(output_path, index=False)
    print(f"Saved silver table: {output_path}")

def run_silver_transform():
    df = load_bronze()
    if df is None:
        return

    motion = clean_motion(df)
    temp_humidity = clean_temp_humidity(df)
    smart_plug = clean_smart_plug(df)
    smoke_co = clean_smoke_co(df)

    save_silver(motion, "silver_motion")
    save_silver(temp_humidity, "silver_temperature_humidity")
    save_silver(smart_plug, "silver_smart_plug")
    save_silver(smoke_co, "silver_smoke_co")

    print("Silver transformation complete.")

if __name__ == "__main__":
    run_silver_transform()