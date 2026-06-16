# UberEats Real-Time Analytics Pipeline

## Overview

This module implements a complete real-time analytics pipeline for UberEats delivery operations using Databricks Delta Live Tables (Lakeflow). It demonstrates the medallion architecture (Bronze, Silver, Gold, Sink) with streaming data processing, quality monitoring, and external system integration via Kafka.

## Business Context

Track UberEats delivery operations in real-time to:
- Monitor restaurant and driver performance with 10-minute windowed metrics
- Identify critically delayed deliveries (>45 min) for immediate operational intervention
- Analyze delivery speed distribution patterns across time buckets
- Provide executive dashboards with platform-wide health metrics
- Alert external systems (Kafka) when critical delays occur for PagerDuty/monitoring integration

## Architecture

```
Cloud Storage (JSON files)
    |
    v
BRONZE LAYER - Raw Data Ingestion
    |-- bronze_orders (Python Auto Loader)
    |-- bronze_status (SQL streaming)
    |
    v
SILVER LAYER - Integration & Enrichment
    |-- silver_order_status (stream-stream join with watermarking)
    |-- silver_delayed_orders (multi-criteria filtered alerts)
    |
    v
GOLD LAYER - Analytics Aggregations
    |-- gold_restaurant_performance (10-min windowed)
    |-- gold_driver_performance (cumulative)
    |-- gold_delivery_time_distribution (time bucketing)

