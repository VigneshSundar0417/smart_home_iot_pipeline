Smart Home IoT Data Engineering Pipeline (Python + Databricks)
This project is a complete end‑to‑end real‑time IoT data engineering pipeline built using Python and the Databricks Lakehouse Platform. It simulates eight smart‑home rooms with four sensor types, ingests events continuously, processes them through bronze, silver, and gold layers, and prepares analytics for dashboards.
The project demonstrates real production skills including streaming ingestion, Delta Lake tables, Databricks notebooks, and workflow automation.

Project Architecture
                         +-----------------------------+
                         |   Real-Time Sensor Simulator |
                         |  (Python, 8 rooms, 4 sensors)|
                         +--------------+--------------+
                                        |
                                        v
                         +-----------------------------+
                         |     Bronze Raw Storage       |
                         |   (JSON files in DBFS/S3)    |
                         +--------------+--------------+
                                        |
                                        v
                         +-----------------------------+
                         |      Bronze Delta Table      |
                         |   smart_home.bronze.events   |
                         +--------------+--------------+
                                        |
                                        v
                         +-----------------------------+
                         |       Silver Delta Tables    |
                         |  motion                      |
                         |  temperature_humidity        |
                         |  smart_plug                  |
                         |  smoke_co                    |
                         +--------------+--------------+
                                        |
                                        v
                         +-----------------------------+
                         |        Gold Delta Tables     |
                         |  Hourly room-level metrics    |
                         |  (motion, temp/humidity,      |
                         |   energy, smoke/co)           |
                         +--------------+--------------+
                                        |
                                        v
                         +-----------------------------+
                         |     Power BI / SQL Warehouse |
                         |     Dashboards & Analytics   |
                         +-----------------------------+
Local Components
- Python real‑time sensor simulator
- Local bronze ingestion script
- Local silver and gold transformation scripts
- Parquet storage for local development
Databricks Components
- Unity Catalog with bronze, silver, and gold schemas
- Delta Lake tables for each layer
- Databricks notebooks for ingestion and transformations
- Databricks Jobs for workflow orchestration
- Databricks SQL Warehouse for BI dashboards

Folder Structure
smart_home_iot_pipeline/
│
├── README.md
├── requirements.txt
│
├── src/
│   ├── simulator/
│   │   └── sensor_simulator.py
│   ├── bronze/
│   │   └── bronze_ingest.py
│   ├── silver/
│   │   └── silver_transform.py
│   └── gold/
│       └── gold_aggregate.py
│
├── notebooks/
│   ├── bronze_ingest_databricks.ipynb
│   ├── silver_transform_databricks.ipynb
│   └── gold_aggregate_databricks.ipynb
│
├── data/
│   ├── bronze_raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── dashboard/
    └── powerbi_dashboard.pbix



Rooms Simulated
- living_room
- kitchen
- bedroom_1
- bedroom_2
- hallway
- office
- garage
- dining_room

Sensor Types
Motion
Temperature and Humidity
Smart Plug (energy usage)
Smoke and CO

Pipeline Flow
1. Real‑Time Sensor Simulator (Local)
Generates continuous JSON events and writes them to:
data/bronze_raw/


2. Bronze Layer
Local script:
- Reads raw JSON files
- Adds ingest timestamp
- Appends to Parquet file:
data/bronze/bronze_events.parquet
- Deletes raw files after ingestion
Databricks notebook:
- Reads JSON from DBFS or cloud storage
- Writes to Delta table:
smart_home.bronze.events


3. Silver Layer
Local script:
- Splits bronze data by sensor type
- Cleans and enforces schema
- Writes four Parquet files
Databricks notebook:
- Writes four Delta tables:
- silver_motion
- silver_temperature_humidity
- silver_smart_plug
- silver_smoke_co
4. Gold Layer
Local script:
- Performs hourly room‑level aggregations
- Writes gold Parquet files
Databricks notebook:
- Writes gold Delta tables:
- gold_motion
- gold_temperature_humidity
- gold_smart_plug
- gold_smoke_co

Databricks Workflow
A Databricks Job orchestrates the pipeline:
- Bronze ingestion notebook
- Silver transformation notebook
- Gold aggregation notebook
- Dashboard refresh (Power BI or Databricks SQL)
This mirrors real enterprise IoT data engineering pipelines.

Power BI Dashboard
The dashboard visualizes:
- Hourly motion activity
- Temperature and humidity trends
- Energy usage patterns
- Smoke and CO safety metrics
- Room‑level comparisons
- Anomaly detection
Power BI connects directly to Databricks SQL Warehouse.

Tech Stack
Python
Databricks Lakehouse
Delta Lake
Unity Catalog
Databricks Jobs
Pandas
Parquet
Power BI

Purpose
This project demonstrates real‑world data engineering capabilities:
- Real‑time IoT simulation
- Multi‑layer Lakehouse architecture
- Delta Lake schema enforcement
- Databricks notebook workflows
- Gold‑level analytics for BI
- Clean, production‑ready GitHub structure
