# TPC-H ETL Pipeline & Data Warehouse

## Overview
This project implements a scalable ETL pipeline for the TPC-H benchmark dataset using **Databricks** and **Delta Lake**. The pipeline extracts eight relational tables, enforces data quality, performs transformations, and produces a **denormalized fact table** optimized for analytics and BI dashboards. The workflow supports customer- and supplier-level insights with minimal manual intervention.

**Interactive Results:** Open `TPCH_ETL_for_analytics.html` in **Databricks** or your browser to explore visual dashboards and query outputs.

---

## Table of Contents
- [Data Sources](#data-sources)  
- [Architecture & Technology Stack](#architecture--technology-stack)  
- [Pipeline Workflow](#pipeline-workflow)  
- [Analytical Queries](#analytical-queries)  
- [Denormalization Strategy](#denormalization-strategy)  
- [How to Run](#how-to-run)  
- [Key Benefits](#key-benefits)  
- [References](#references)  

---

## Data Sources
**TPC-H Benchmark Dataset**  
- Source: Databricks Sample Datasets  
- Location: `dbfs:/databricks-datasets/tpch/data-001/`  
- Format: Pipe-delimited CSV files  
- Scale Factor: 0.001 (~1MB)  

**Tables & Records**

| Table | Records | Description |
|-------|--------|-------------|
| customer | 150 | Customer master data with account and geographic info |
| orders | 1,500 | Order transactions with dates, status, totals |
| lineitem | 6,005 | Line items with pricing, quantities, shipping |
| part | 200 | Product catalog |
| supplier | 10 | Supplier master data |
| partsupp | 800 | Part-supplier relationships (availability, cost) |
| nation | 25 | Country reference data |
| region | 5 | Geographic regions (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST) |

**Country Code Reference Data**  
- Source: CountryCode.org  
- Purpose: Standardizes nation names to ISO3 codes for geographic analytics  

---

## Architecture & Technology Stack
- **Platform:** Databricks  
- **Storage:** Delta Lake  
- **Processing:** PySpark & SQL  
- **Catalog Structure:** `TPCH_ETL_catalog` → `TPCH_schema`  
- **Volumes:** input, output  

---

## Pipeline Workflow
1. **Data Exploration**  
   - Inspect CSV files, analyze schema, and preview data.  
   - Load into PySpark DataFrames and create temporary views.  

2. **Data Modeling & Quality**  
   - Create Delta catalog and table schemas.  
   - Enforce constraints (e.g., `quantity > 0`, `discount 0–1`).  

3. **Ingestion**  
   - Read CSVs with defined schemas.  
   - Convert date strings to `DATE` type and `line_status` to boolean.  
   - Write tables to Delta format.  

4. **Transformation**  
   - Map nation names to ISO3 country codes.  
   - Join tables to enrich geographic data (`nationmapped`).  

5. **Analytical Queries**  
   - **Top 50 active customers** in the last 30 days  
   - **Top 10 suppliers** by account balance  
   - **3 cheapest suppliers per part**  

6. **Denormalization**  
   - Combine customer, supplier, part, lineitem, and order tables into `denormalizedDF`.  
   - Adds geographic hierarchy and merges product-supplier data.  

---

## How to Run
1. Open **Databricks** workspace.  
2. Upload `TPCH_ETL_for_analytics.html` to Databricks or your local browser.  
3. Open `TPCH_ETL_for_analytics.html` to interact with dashboards.  
4. Optional: run notebook cells sequentially to rebuild tables or perform new analysis.  

---

## Denormalization Strategy
**Flow Diagram**  
# Flow Diagram
```
Customer ──┐                    Supplier ──┐
           │                               │
           ▼                               ▼
         Nation ──► Region               Nation ──► Region
           │                               │
           ▼                               ▼
        cust_geo                        supp_geo
           │                               │
           └───────────┐       ┌───────────┘
                       │       │
Lineitem ──► Orders    │       │    Part ──► Partsupp
               │       │       │               │
               ▼       │       │               ▼
      order_lineitem   │       │         part_partsupp
               │       │       │               │
               └───────┼───────┼───────────────┘
                       │       │
                       ▼       ▼
                     denormalizedDF
```
  ---
  
## Next Steps / Future Enhancements

- **Increase Scale Factor**: Test the pipeline on larger datasets (SF 0.01, SF 1, etc.) to evaluate performance and scalability.
- **Optimize Transformations**: Implement Spark partitioning, caching, and broadcast joins to improve ETL speed on larger datasets.
- **Expand Analytics**: Create additional business queries and KPIs on top of the denormalized fact table.
- **Integrate with BI Tools**: Connect the denormalized table to Tableau, Power BI, or Databricks dashboards for real-time reporting.
- **Experiment with Machine Learning**: Use enriched denormalized data to train ML models for sales forecasting, supplier optimization, or anomaly detection.
