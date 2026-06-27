# Auto CDC API Demo - Simplified Pipeline

**Purpose:** Demonstrates Databricks Auto CDC API capabilities with minimal configuration.

**Official Documentation:** https://docs.databricks.com/aws/en/ldp/cdc?language=Python

---

## Architecture

```
MongoDB CDC Events ──┐
                     ├─→ [BRONZE] ─→ [SILVER CDC] ─→ [GOLD Analytics]
MSSQL CDC Events ────┘   Streaming    apply_changes()   Live Tables
                         Tables       Auto CDC API
```

---

## Pipeline Files

### Bronze Layer (Raw CDC Ingestion)
- **mongodb-users.sql** - MongoDB CDC events via Auto Loader
- **mssql-users.sql** - MSSQL CDC events via Auto Loader

### Silver Layer (Auto CDC Processing)
- **users-cdc.py** - Auto CDC with `apply_changes()`
  - Creates: `silver_users_current` (SCD Type 1)
  - Creates: `silver_users_history` (SCD Type 2)

### Gold Layer (Analytics)
- **user-analytics.sql** - Business analytics on CDC data
  - Demographics by city
  - Change audit trail

---