"""
BRONZE LAYER - Order Creation Events Ingestion

PURPOSE:
This module ingests raw order creation events from cloud storage into the Delta Lake bronze layer.
It implements the Auto Loader pattern for scalable, fault-tolerant streaming ingestion of JSON files.

WHAT IT DOES:
- Monitors cloud storage for new order JSON files from Kafka topics
- Automatically infers and evolves schema as data arrives
- Applies schema hints for critical columns (timestamps, decimals)
- Adds metadata columns for data lineage tracking
- Creates a streaming Delta table for downstream consumption

DATA FLOW:
  Cloud Storage (JSON files)
    -> Auto Loader (cloudFiles)
    -> Delta Lake (bronze_orders table)
    -> Available for Silver layer transformation

KEY FEATURES:
- Schema inference: Automatically detects column types from JSON
- Schema evolution: Adapts to new fields without manual intervention
- Checkpointing: Ensures exactly-once processing semantics
- Metadata tracking: Captures source file path and ingestion timestamp
- Change Data Feed: Enables incremental processing downstream

LEARNING OBJECTIVES:
- Understand Auto Loader (cloudFiles) pattern for cloud ingestion
- Learn schema inference and hint mechanisms
- Implement metadata enrichment for lineage tracking
- Configure Delta Lake properties for streaming workloads

CONFIGURATION:
- source_path: Cloud storage path containing JSON files
- checkpoint_location: Path for streaming checkpoints
- Schema hints ensure critical columns have correct types

OUTPUT SCHEMA:
- All columns from source JSON files (auto-detected)
- ingestion_timestamp: When record was ingested (TIMESTAMP)
- source_file: Origin file path for lineage (STRING)
"""

import dlt
from pyspark.sql import functions as F

SOURCE_PATH = spark.conf.get("source_path","/Volumes/uber/delivery/files")
WORK_PATH = spark.conf.get("work_path","/Volumes/uber/delivery/confs/")


@dlt.table(
  name = "bronze_orders",
  comment = "Raw orders creation from kafka",
  table_properties = {
    "quality": "bronze",
    "layer": "ingestion",
    "source": "kafka",
    "topic": "orders",
    "delta.enableChangeDataFeed": "true"
  }
)
def bronze_orders():
  """
  Ingest raw order creation events from cloud storage using Auto Loader.

    This function implements the bronze layer pattern for raw data ingestion.
    It uses Databricks Auto Loader (cloudFiles) to efficiently process JSON files
    from cloud storage with automatic schema inference and evolution.

    Schema Inference Strategy:
        - Automatically detects column names and types from JSON structure
        - Applies explicit hints for business-critical columns to ensure correctness
        - Stores inferred schema in checkpoint location for consistency

    Schema Hints Applied:
        - order_date: TIMESTAMP (prevents string interpretation)
        - dt_current_timestamp: TIMESTAMP (event time from source system)
        - total_amount: DECIMAL(10,2) (ensures precision for monetary values)

    Metadata Enrichment:
        - ingestion_timestamp: Current time when Spark processes the record
        - source_file: Full path to the source JSON file for lineage tracking

    Returns:
        DataFrame: Streaming DataFrame containing all source columns plus metadata

    Streaming Characteristics:
        - Mode: Append-only (new files trigger incremental processing)
        - Checkpointing: Automatic via cloudFiles framework
        - Fault Tolerance: Exactly-once semantics guaranteed
        - Schema Evolution: New columns automatically added to table
  """  
  return(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes","true")
    .option("cloudFiles.schemaLocation",f"{WORK_PATH}/schema/bronze_orders")
    .option("cloudFiles.schemaHints",
            "order_date timestamp, dt_current_timestamp timestamp, total_amount decimal(10,2)")
    .load(f"{SOURCE_PATH}/orders/*.json")
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path"))
  )