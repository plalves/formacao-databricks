# Restaurant Analytics Pipeline

**End-to-end analytics platform for multi-source restaurant data using Delta Live Tables.**

---

## 📋 Use Case Summary

### Business Problem
Restaurant operations teams need unified analytics across multiple data sources (MySQL, PostgreSQL) to:
- Monitor restaurant performance and customer satisfaction
- Track product inventory and profitability across locations
- Identify top-performing cuisines and restaurants
- Optimize inventory management and reduce waste

### Solution
Medallion architecture (Bronze → Silver → Gold) pipeline that:
1. **Ingests** multi-source data from Storage (MySQL restaurants/ratings/products + PostgreSQL inventory)
2. **Cleanses** and validates data with quality constraints
3. **Transforms** into star schema for BI tools (Power BI, Tableau)
4. **Delivers** pre-aggregated analytics for dashboards

### Business Value
- **360° Restaurant View**: Unified customer ratings, inventory, and performance metrics
- **Real-time Insights**: Streaming pipeline with Auto Loader for incremental updates
- **Data Quality**: Automated validation ensures trustworthy analytics
- **Self-Service Analytics**: Star schema optimized for business users

---

## 📊 Tables Involved

### Source Systems
| System | Table | Records | Key Fields |
|--------|-------|---------|------------|
| **MySQL** | restaurants | ~100 | restaurant_id, cnpj, cuisine_type, ratings |
| **MySQL** | ratings | ~10K | rating_id, restaurant_identifier (cnpj), rating |
| **MySQL** | products | ~5K | product_id, restaurant_id, price, cost |
| **PostgreSQL** | inventory | ~5K | stock_id, restaurant_id, product_id, quantity |

### Pipeline Layers (13 Tables)

#### Bronze Layer (4 tables - Raw)
```
bronze_restaurants   → All restaurant master data (no filtering)
bronze_ratings       → All customer ratings (no filtering)
bronze_products      → All menu items (no filtering)
bronze_inventory     → All inventory snapshots (no filtering)
```

#### Silver Layer (4 tables - Cleaned)
```
silver_restaurants   → Validated restaurants (nulls dropped)
silver_ratings       → Valid ratings 0-5 only
silver_products      → Valid products with profit_margin calculated
silver_inventory     → Valid inventory (quantity >= 0)
```

#### Gold Layer (5 tables - Analytics)

**Dimensions (SCD Type 1):**
```
dim_restaurant       → Current restaurant attributes
dim_product          → Current product catalog
```

**Facts (Denormalized):**
```
fact_ratings         → Ratings + restaurant info (for trend analysis)
fact_inventory       → Inventory + product + restaurant (for stock analysis)
```

**Analytics (Pre-aggregated):**
```
gold_restaurant_summary  → Avg rating, review counts, satisfaction %
gold_product_summary     → Inventory value, restaurant coverage
gold_cuisine_summary     → Performance by cuisine type
```

---

## 🔄 Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│ SOURCE: Azure Blob Storage                                   │
│ abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ BRONZE LAYER - Auto Loader (Streaming)                       │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────┐│
│ │ restaurants │ │   ratings   │ │  products   │ │inventory││
│ │  (MySQL)    │ │   (MySQL)   │ │  (MySQL)    │ │(Postgres)││
│ └─────────────┘ └─────────────┘ └─────────────┘ └─────────┘│
│ • Schema inference   • _metadata tracking   • No transforms │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ SILVER LAYER - Quality Checks (Streaming)                    │
│ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────┐│
│ │ restaurants │ │   ratings   │ │  products   │ │inventory││
│ │ CONSTRAINT  │ │ CONSTRAINT  │ │ CONSTRAINT  │ │CONSTRAINT││
│ │ DROP invalid│ │ DROP invalid│ │ DROP invalid│ │DROP     ││
│ └─────────────┘ └─────────────┘ └─────────────┘ └─────────┘│
│ • Trim/uppercase   • Rating 0-5 only   • Profit margin calc │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ GOLD LAYER - Star Schema (Batch)                             │
│                                                               │
│ DIMENSIONS              FACTS                  ANALYTICS      │
│ ┌─────────────┐        ┌──────────────┐      ┌────────────┐ │
│ │dim_restaurant│◄───────┤fact_ratings  │──────►restaurant  │ │
│ │             │        │              │      │summary     │ │
│ │dim_product  │◄───────┤fact_inventory│──────►product     │ │
│ │             │        │              │      │summary     │ │
│ └─────────────┘        └──────────────┘      │cuisine     │ │
│                                               │summary     │ │
│                                               └────────────┘ │
│ • SCD Type 1    • Denormalized joins    • Pre-aggregated    │
└─────────────────────────────────────────────────────────────┘
                          ↓
                  ┌──────────────┐
                  │   BI TOOLS   │
                  │ Power BI     │
                  │ Tableau      │
                  │ Dashboards   │
                  └──────────────┘
```

### Join Keys
- `restaurant_id` → Primary key across all restaurant-related tables
- `cnpj` → Business identifier linking ratings to restaurants
- `product_id` + `restaurant_id` → Composite key for inventory

---

## 🛠️ Technical Specification

### Pipeline Configuration

| Component | Specification |
|-----------|--------------|
| **Framework** | Delta Live Tables (DLT) |
| **Language** | SQL (declarative) |
| **Ingestion** | Auto Loader (cloud_files) |
| **Source Format** | JSON |
| **Storage** | Azure Blob Storage (ADLS Gen2) |
| **Target** | Unity Catalog schema |
| **Compute** | Serverless recommended (or 1-5 worker cluster) |
| **Mode** | Triggered (or Continuous for real-time) |

### Files Structure
```
01_bronze.sql           → 4 STREAMING LIVE TABLEs (Auto Loader)
02_silver.sql           → 4 STREAMING LIVE TABLEs (Quality checks)
03_gold_dimensions.sql  → 2 LIVE TABLEs (SCD Type 1)
04_gold_facts.sql       → 2 LIVE TABLEs (Denormalized joins)
05_gold_analytics.sql   → 3 LIVE TABLEs (Aggregations)
```

### Data Quality Rules

| Layer | Policy | Count | Examples |
|-------|--------|-------|----------|
| **Bronze** | EXPECT (warn) | 0 | Track schema issues, no drops |
| **Silver** | EXPECT ON VIOLATION DROP ROW | 8 | `rating BETWEEN 0 AND 5`, `price > 0` |
| **Gold** | EXPECT ON VIOLATION FAIL | 5 | `restaurant_id IS NOT NULL` (PKs) |

### Key Transformations

**Silver Layer:**
- Trim and uppercase text fields (CNPJ normalization)
- Calculate `profit_margin = price - unit_cost`
- Validate business rules (ratings 0-5, positive quantities)

**Gold Layer:**
- Join ratings to restaurants via CNPJ
- Denormalize dimensions into facts for query performance
- Pre-aggregate metrics (AVG rating, inventory value, satisfaction %)

### Performance Optimizations
- **Auto Optimize**: Enabled on all tables
- **Z-Ordering**: Applied to primary/foreign keys
- **Partitioning**: Not needed for small datasets (<1TB)
- **Photon**: Enable for 3-5x speedup
- **Streaming**: Bronze/Silver for incremental processing
- **Batch**: Gold for cost-effective aggregations

---