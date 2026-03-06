import os
import json
import time
from datetime import datetime
import pandas as pd

RAW_FOLDER = "/home/vignesh/smart_home_iot_pipeline/data/bronze_raw/"
BRONZE_FOLDER = "/home/vignesh/smart_home_iot_pipeline/data/bronze/"

os.makedirs(BRONZE_FOLDER, exist_ok=True)

def process_file(file_path):
    try:
        with open(file_path, "r") as f:
            event = json.load(f)

        event["ingest_timestamp"] = datetime.utcnow().isoformat()

        df = pd.DataFrame([event])

        bronze_file = os.path.join(BRONZE_FOLDER, "bronze_events.parquet")

        if os.path.exists(bronze_file):
            df_existing = pd.read_parquet(bronze_file)
            df = pd.concat([df_existing, df], ignore_index=True)

        df.to_parquet(bronze_file, index=False)

        os.remove(file_path)

        print(f"Ingested: {file_path}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def run_bronze_ingestion():
    print("Bronze ingestion started... watching folder for new events.")

    while True:
        files = [f for f in os.listdir(RAW_FOLDER) if f.endswith(".json")]

        for file_name in files:
            file_path = os.path.join(RAW_FOLDER, file_name)
            process_file(file_path)

        time.sleep(1)

if __name__ == "__main__":
    run_bronze_ingestion()