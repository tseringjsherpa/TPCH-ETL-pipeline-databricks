# TPC-H ETL Pipeline & Data Warehouse

## Overview

This project implements an ETL pipeline for the TPC-H benchmark dataset using Databricks and Delta Lake. The pipeline extracts eight relational tables, performs data cleaning and transformation, executes analytical queries, and creates a denormalized fact table optimized for business intelligence queries.

> **Note:** The file `TPCH_ETL_for_analytics.html` is the interactive HTML version of the notebook with visualizations. GitHub cannot render it due to file size, so download it locally to explore the interactive dashboards.

---

## Table of Contents

1. [Data Sources](#data-sources)
2. [Architecture](#architecture)
3. [Pipeline Workflow](#pipeline-workflow)
4. [Denormalization Strategy](#denormalization-strategy)
5. [References](#references)

---

## Data Sources

### TPC-H Benchmark Dataset

**Source:** Databricks Sample Datasets  
**Location:** `dbfs:/databricks-datasets/tpch/data-001/`  
**Format:** Pipe-delimited CSV files  
**Scale Factor:** 0.001 (~1MB)

The dataset contains eight tables simulating a wholesale supplier's operations:

| Table | Records | Description |
|-------|---------|-------------|
| **customer** | 150 | Customer master data with account and geographic information |
| **orders** | 1,500 | Order transactions with dates, status, and totals |
| **lineitem** | 6,005 | Order line items with pricing, quantities, and shipping details |
| **part** | 200 | Product catalog with specifications |
| **supplier** | 10 | Supplier master data |
| **partsupp** | 800 | Part-supplier relationships with availability and cost |
| **nation** | 25 | Country reference data |
| **region** | 5 | Geographic regions (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST) |

### Country Code Reference Data

**Source:** [CountryCode.org](https://countrycode.org/)  
**Purpose:** Maps nation names to ISO 3166-1 alpha-3 country codes for standardized geographic analysis

---

## Architecture

**Platform:** Databricks  
**Storage:** Delta Lake  
**Processing:** PySpark & SQL  
**Catalog Structure:**
- Catalog: `TPCH_ETL_catalog`
- Schema: `TPCH_schema`
- Volumes: `input`, `output`

---

## Pipeline Workflow

### 1. Data Exploration
- List and inspect source files
- Load tables into DataFrames with temporary views
- Analyze schema and sample data

### 2. Data Modeling
- Create Delta Lake catalog and schema
- Define table schemas with primary and foreign keys
- Add check constraints for data quality:
  - Quantity > 0
  - Discount between 0 and 1
  - Valid return flags and market segments

### 3. Ingestion
- Read CSV files with defined schemas
- Convert data types:
  - String dates to DATE type (`order_date`, `ship_date`, `commit_date`, `receipt_date`)
  - `line_status` from string ('O'/'F') to boolean
- Write tables to Delta format

### 4. Transformation
- Load country code mapping from CountryCode.org
- Join Nation table with country codes to create `nationmapped` table
- Enrich nation data with ISO3 country identifiers

### 5. Analytical Queries

Three key business queries executed on normalized data:

**Query 1:** Top 50 active customers by orders in the last 30 days
```sql
SELECT c.cust_key, c.name, nm.iso, COUNT(DISTINCT o.order_key)
FROM customer c
JOIN orders o ON c.cust_key = o.cust_key
JOIN nationmapped nm ON c.nation_key = nm.nation_key
WHERE order_date >= last_order_date - 30
GROUP BY c.cust_key, c.name, nm.iso
ORDER BY total_orders DESC
LIMIT 50
```

**Query 2:** Top 10 suppliers by account balance
```sql
SELECT s.supp_key, s.name, s.acct_bal, nm.iso
FROM supplier s
JOIN nationmapped nm ON s.nation_key = nm.nation_key
ORDER BY s.acct_bal DESC
LIMIT 10
```

**Query 3:** 3 cheapest suppliers per part
```sql
SELECT part_key, supp_key, supply_cost, rank
FROM (
  SELECT part_key, supp_key, supply_cost,
         ROW_NUMBER() OVER (PARTITION BY part_key ORDER BY supply_cost) AS rank
  FROM partsupp
)
WHERE rank <= 3
```

### 6. Denormalization

The denormalization process combines related tables in four steps:

1. **`cust_geo`:** Customer → Nation → Region (adds geographic hierarchy)
2. **`supp_geo`:** Supplier → Nation → Region (adds geographic hierarchy)
3. **`order_lineitem`:** Lineitem → Orders (combines line and order details)
4. **`part_partsupp`:** Part → Partsupp (merges product and supplier info)

#### Final Join

```python
denormalizedDF = (
    order_lineitem
    .join(cust_geo, "cust_key", "left")
    .join(supp_geo, "supp_key", "left")
    .join(part_partsupp, ["part_key", "supp_key"], "inner")
)
```

**Result:** Single table with 6,005 rows containing all customer, supplier, part, order, and geographic information.

---

## Denormalization Strategy

### Flow Diagram

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
          (Complete Fact Table)
```

### Benefits

- **Simplified Queries:** Single table eliminates complex joins
- **Performance:** Faster query execution for analytical workloads
- **Business Intelligence:** Ready for BI tools and dashboards
- **Complete Context:** All dimensions available in one table

---

## References

1. Transaction Processing Performance Council. (n.d.). *TPC Benchmark™ H Standard Specification, Revision 2.17.1*. Retrieved from https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
3. CountryCode.org. (n.d.). *Country codes - ISO 3166*. Retrieved from https://countrycode.org/
