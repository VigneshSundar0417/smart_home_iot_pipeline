import os
import json
import time
import random
from datetime import datetime

OUTPUT_FOLDER = "/home/vignesh/smart_home_iot_pipeline/data/bronze_raw/"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

rooms = [
    "living_room",
    "kitchen",
    "bedroom_1",
    "bedroom_2",
    "hallway",
    "office",
    "garage",
    "dining_room"
]

def generate_motion_event(room):
    return {
        "sensor_type": "motion",
        "room": room,
        "motion_detected": random.choice([True, False]),
        "confidence": round(random.uniform(0.5, 1.0), 2),
        "event_ts": datetime.utcnow().isoformat()
    }

def generate_temp_humidity_event(room):
    return {
        "sensor_type": "temperature_humidity",
        "room": room,
        "temperature_c": round(random.uniform(18.0, 28.0), 2),
        "humidity_percent": round(random.uniform(30.0, 60.0), 2),
        "event_ts": datetime.utcnow().isoformat()
    }

def generate_smart_plug_event(room):
    return {
        "sensor_type": "smart_plug",
        "room": room,
        "watt_hours": round(random.uniform(5.0, 50.0), 2),
        "voltage": round(random.uniform(110.0, 120.0), 2),
        "current_amps": round(random.uniform(0.1, 1.5), 2),
        "event_ts": datetime.utcnow().isoformat()
    }

def generate_smoke_co_event(room):
    return {
        "sensor_type": "smoke_co",
        "room": room,
        "smoke_level": random.randint(0, 5),
        "co_level": random.randint(0, 5),
        "battery_level": random.randint(50, 100),
        "event_ts": datetime.utcnow().isoformat()
    }

def write_event(event):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{event['sensor_type']}_{timestamp}.json"
    filepath = os.path.join(OUTPUT_FOLDER, filename)

    with open(filepath, "w") as f:
        json.dump(event, f)

    print(f"Generated: {filename}")

def run_simulator():
    print("Real-time sensor simulator started...")

    while True:
        room = random.choice(rooms)

        event_type = random.choice([
            generate_motion_event,
            generate_temp_humidity_event,
            generate_smart_plug_event,
            generate_smoke_co_event
        ])

        event = event_type(room)
        write_event(event)

        time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    run_simulator()