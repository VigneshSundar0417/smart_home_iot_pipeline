import os
import pandas as pd

SILVER_FOLDER = "/home/vignesh/smart_home_iot_pipeline/data/silver/"
GOLD_FOLDER = "/home/vignesh/smart_home_iot_pipeline/data/gold/"

os.makedirs(GOLD_FOLDER, exist_ok=True)

def load_silver(name):
    path = os.path.join(SILVER_FOLDER, name)
    if not os.path.exists(path):
        print(f"Missing silver file: {name}")
        return None
    return pd.read_parquet(path)

def aggregate_motion(df):
    df["event_hour"] = df["event_ts"].dt.floor("H")
    grouped = df.groupby(["room", "event_hour"]).agg({
        "motion_detected": "sum",
        "confidence": "mean",
        "event_ts": "max"
    }).reset_index()
    grouped.rename(columns={
        "motion_detected": "motion_count",
        "confidence": "avg_confidence",
        "event_ts": "last_event_ts"
    }, inplace=True)
    return grouped

def aggregate_temp_humidity(df):
    df["event_hour"] = df["event_ts"].dt.floor("H")
    grouped = df.groupby(["room", "event_hour"]).agg({
        "temperature_c": ["mean", "min", "max"],
        "humidity_percent": ["mean", "min", "max"]
    })
    grouped.columns = ["_".join(col) for col in grouped.columns]
    return grouped.reset_index()

def aggregate_smart_plug(df):
    df["event_hour"] = df["event_ts"].dt.floor("H")
    grouped = df.groupby(["room", "event_hour"]).agg({
        "watt_hours": "sum",
        "voltage": "mean",
        "current_amps": "mean"
    }).reset_index()
    grouped.rename(columns={
        "watt_hours": "total_watt_hours",
        "voltage": "avg_voltage",
        "current_amps": "avg_current"
    }, inplace=True)
    return grouped

def aggregate_smoke_co(df):
    df["event_hour"] = df["event_ts"].dt.floor("H")
    grouped = df.groupby(["room", "event_hour"]).agg({
        "smoke_level": "mean",
        "co_level": "mean",
        "battery_level": "min"
    }).reset_index()
    grouped.rename(columns={
        "smoke_level": "avg_smoke_level",
        "co_level": "avg_co_level",
        "battery_level": "min_battery_level"
    }, inplace=True)
    return grouped

def save_gold(df, name):
    path = os.path.join(GOLD_FOLDER, f"{name}.parquet")
    df.to_parquet(path, index=False)
    print(f"Saved gold table: {path}")

def run_gold_aggregation():
    motion = load_silver("silver_motion.parquet")
    temp = load_silver("silver_temperature_humidity.parquet")
    plug = load_silver("silver_smart_plug.parquet")
    smoke = load_silver("silver_smoke_co.parquet")

    if motion is not None:
        save_gold(aggregate_motion(motion), "gold_motion")

    if temp is not None:
        save_gold(aggregate_temp_humidity(temp), "gold_temperature_humidity")

    if plug is not None:
        save_gold(aggregate_smart_plug(plug), "gold_smart_plug")

    if smoke is not None:
        save_gold(aggregate_smoke_co(smoke), "gold_smoke_co")

    print("Gold aggregation complete.")

if __name__ == "__main__":
    run_gold_aggregation()