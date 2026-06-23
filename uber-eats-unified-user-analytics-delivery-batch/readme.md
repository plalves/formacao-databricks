# Batch CDC Implementation (Simplified Declarative Pattern)

## 📋 What's in This Folder

This folder contains the **Batch CDC** implementation using a **simplified declarative pattern** for snapshot-based change tracking.

## 🔄 CDC Pattern: Snapshot-Based Tracking

**How it works:**
1. Source systems export **full snapshots** to blob storage (hourly/daily)
2. Bronze layer ingests complete snapshots as MATERIALIZED VIEWS
3. Silver staging unifies data with FULL OUTER JOIN
4. **Simplified declarative pattern** tracks changes by appending snapshots
5. DLT automatically handles table creation and persistence

**Key Characteristic:** You have **full data dumps**, not individual CDC events

## 🎯 When to Use Batch CDC

✅ **Use Batch CDC when you have:**
- Full snapshots exported from source systems (JSON, CSV, Parquet dumps)
- Hourly/daily data refreshes (not real-time)
- Slowly changing dimension data (users, products, customers)
- Lower volume data (<10M records per refresh)
- Cost optimization priority (batch is 60-80% cheaper)

❌ **Don't use Batch CDC when:**
- You have true CDC streams (Debezium, SQL Server CDC, Oracle GoldenGate)
- You need sub-second latency
- You have high-volume transactional data
- Source systems can send only changed records