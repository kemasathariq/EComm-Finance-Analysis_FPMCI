# DustiniaDelixia Groceria — Finance Analyst Pipeline

Final Project Lab MCI 2026 | Persona 1: Finance Analyst | ITS Surabaya

---

## Business Problem

The Head of Finance at DustiniaDelixia Groceria identified a significant gap in quarterly spend data: most customers transact at average values, but a specific segment spends far above average. This segment had never been analyzed from a payment behavior perspective.

This pipeline was built to answer four questions:

1. Who are the high-value customers (top 25% by total spend)?
2. What payment methods do they prefer?
3. Where are they located geographically?
4. Where is the company losing potential revenue due to suboptimal payment options?

---

## Key Findings

| Finding | Value |
|---|---|
| High-value customer threshold | R$ 180.52 total spend (75th percentile) |
| High-value customers | 23,603 out of 94,401 (25%) |
| Revenue from high-value customers | R$ 9.27M out of R$ 15.6M total (59.4%) |
| Dominant payment method (HVC) | Credit card |
| Installment impact | 12+ installment users spend R$ 410/order vs R$ 120 for no installments (3.4×) |
| Highest leakage state | São Paulo — credit card orders 10.4% below national average (score: 2,551) |
| Repeat customer rate (HVC) | 7.6% |
| Repeat customer rate (Regular) | 1.5% |

---

## Architecture

```
CSV Files (11 sources)
        |
        v
[Apache Airflow 2.8.1]
DAG: dustinia_finance_analyst_pipeline
        |
        v
[ClickHouse 23.8]              <- columnar analytics engine
raw_* tables (11)              <- exact mirrors of source CSVs
        |
mart_* tables (11)             <- transformed, analytics-ready
        |
        v
[PostgreSQL 15]                <- mart mirror for Metabase
schema: dustinia
        |
        v
[Metabase 0.49.0]             <- dashboard for Finance Head
```

Why PostgreSQL as the Metabase bridge: Metabase's ClickHouse community driver is version-locked to specific Metabase releases (driver 1.4.0 for Metabase 0.49.x). To avoid fragile plugin dependencies, mart tables are mirrored to PostgreSQL after each pipeline run. Metabase uses its native PostgreSQL connector.

---

## Stack

| Tool       | Version | Purpose                                            | Port |
|------------|---------|----------------------------------------------------|------|
| Airflow    | 2.8.1   | Pipeline orchestration, scheduling, monitoring     | 8080 |
| ClickHouse | 23.8    | Columnar DB — fast analytical transforms at scale  | 8123 |
| PostgreSQL | 15      | Airflow metadata + Metabase metadata + mart mirror | 5432 |
| Metabase   | 0.49.0  | BI dashboard for non-technical stakeholders        | 3000 |

All services run locally via Docker Compose. Designed for 8GB RAM on Apple Silicon.

---

## Setup

### Prerequisites

- Docker Desktop installed and running
- At least 5GB free RAM
- Dataset from: https://its.id/m/Dataset_FP_MCI

### Step 1: Place Dataset Files

Unzip the dataset into `data/raw/`. Required files:

```
data/raw/
  orders.csv               order_items.csv         order_payments.csv
  customers.csv            products.csv             category_translation.csv
  sellers.csv              geolocation.csv          order_reviews.csv
  mql.csv                  closed_deals.csv
```

### Step 2: First-Time Setup

```bash
make build
make init
```

### Step 3: Start All Services

```bash
make up
```

Wait ~60 seconds for all services to become healthy.

### Step 4: Trigger the Pipeline

```bash
make trigger
```

Or go to http://localhost:8080 (admin/admin) → `dustinia_finance_analyst_pipeline` → play button.
Full pipeline takes approximately 5–6 minutes.

### Step 5: View Dashboard

Open http://localhost:3000

---

## Pipeline Logic — Phase by Phase

### Phase 0: setup_schema
Creates all 22 ClickHouse tables (11 raw + 11 mart) using `IF NOT EXISTS`. Also runs `ALTER TABLE ADD COLUMN IF NOT EXISTS` for schema migrations on existing installations. Runs every execution — all statements are idempotent.

### Phase 1: extract_raw_data
Validates all 11 CSV files exist in `/opt/airflow/data/raw/` before anything else runs. Raises `FileNotFoundError` if any file is missing — prevents partial pipeline execution.

### Phase 2: validate_data
Asserts row counts are nonzero, primary keys have no nulls, payment values are non-negative. Pushes row counts to Airflow XCom for downstream reference.

### Phase 3: load_raw_to_clickhouse
Reads each CSV with pandas, applies source data quality fixes (see Data Quality Issues section), TRUNCATEs the target table, then INSERTs. TRUNCATE before insert makes every re-run produce clean data with no duplicates.

### Phase 4: Transforms (10 parallel tasks)

| Task | Output mart | Depends on | What it computes |
|---|---|---|---|
| transform_customer_profiles | mart_customer_payment_profile | raw load | Customer-level spend, payment preference, HVC flag, installment bucket, avg review score |
| transform_order_payments | mart_order_payment_detail | raw load | Clean order-level join of orders + payments + customers + items |
| transform_geo_summary | mart_geo_payment_summary | customer_profiles | State-level revenue, payment mix, HVC revenue share |
| transform_monthly_revenue | mart_monthly_revenue | order_payments | Revenue grouped by year-month × payment type |
| transform_hv_category_preference | mart_hv_category_preference | customer_profiles + order_payments | Product categories purchased by HVC only |
| transform_seller_acquisition | mart_seller_acquisition | raw load | MQL → closed deal funnel with days_to_close |
| transform_seller_performance | mart_seller_performance | raw load | Per-seller revenue, review score, category breadth |
| transform_repeat_customer | mart_repeat_customer | customer_profiles | Repeat purchase flag and days active per customer |
| transform_product_price_range | mart_product_price_range | raw load | Order volume by month × category × price bucket |
| transform_review_by_segment | mart_review_by_segment | customer_profiles | Avg review score by HVC flag × payment type × state |

### Phase 5: compute_leakage_scores
Reads `mart_order_payment_detail`. Calculates national average order value, groups by state × payment_type, computes leakage score. Negative leakage (above average) is clipped to 0 — only underperformers are flagged.

**Leakage score formula:**
```
leakage_score = (national_avg - local_avg) / national_avg × order_count
```
- `(national_avg - local_avg) / national_avg` = how far below national average this state+payment combo performs, normalized as a percentage
- `× order_count` = scales by volume so high-traffic underperformers rank higher
- Result: a prioritized list of where payment optimization will have the most revenue impact

### Phase 6: export_marts_to_postgres
Reads all 11 mart tables from ClickHouse. Creates `dustinia` schema in PostgreSQL `metabase` database if not exists. Writes each mart with `if_exists='replace'` — idempotent, always reflects the latest pipeline run.

---

## DAG Task Flow

```
setup_schema → extract_raw_data → validate_data → load_raw_to_clickhouse
    ├── transform_customer_profiles → transform_geo_summary ──────────────┐
    ├── transform_order_payments ─────────────────────────────────────────┼──► compute_leakage_scores
    │       ├── transform_monthly_revenue                                  │
    │       └── (+ customer_profiles) ──────────────────────────────────────► transform_hv_category_preference
    ├── transform_seller_acquisition                                        │
    ├── transform_seller_performance                                        │
    ├── (+ customer_profiles) ──► transform_repeat_customer                │
    ├── transform_product_price_range                                       │
    └── (+ customer_profiles) ──► transform_review_by_segment              │
                                                                            │
    [all 10 transforms + leakage complete] ◄────────────────────────────────┘
    └── export_marts_to_postgres
```

---

## ClickHouse Tables

### Raw Layer (11 tables) — mirrors of source CSV files

| Table | Rows | Source File |
|---|---|---|
| raw_orders | 99,441 | orders.csv |
| raw_order_items | 112,650 | order_items.csv |
| raw_order_payments | 103,886 | order_payments.csv |
| raw_customers | 99,441 | customers.csv |
| raw_products | 32,951 | products.csv |
| raw_product_category_translation | 71 | category_translation.csv |
| raw_sellers | 3,095 | sellers.csv |
| raw_geolocation | 1,000,163 | geolocation.csv |
| raw_order_reviews | 99,224 | order_reviews.csv |
| raw_mql | 8,000 | mql.csv |
| raw_closed_deals | 842 | closed_deals.csv |

### Mart Layer (11 tables) — analytics-ready, mirrored to PostgreSQL

| Table | Rows | Business purpose |
|---|---|---|
| mart_customer_payment_profile | 94,401 | Core segmentation table. One row per unique customer. Contains total spend, HVC flag, payment preference, installment behavior, avg review score. |
| mart_order_payment_detail | 99,441 | One row per order. Clean join of all order-related data. Foundation for leakage and monthly trend analysis. |
| mart_geo_payment_summary | 27 | One row per Brazilian state. Revenue totals, payment mix, HVC revenue share. |
| mart_payment_leakage | 107 | One row per state × payment type with leakage score. Prioritized list of where to focus payment optimization. |
| mart_monthly_revenue | 88 | Revenue and order count by year-month × payment type. Shows payment method adoption trends. |
| mart_hv_category_preference | 25,187 | Product categories purchased by HVC. Identifies top categories for targeted promotions. |
| mart_seller_acquisition | 842 | Converted MQL → seller records. Business segment, lead type, days to close. |
| mart_seller_performance | 2,978 | Per-seller revenue, review score, unique categories sold. Identifies top sellers for boost packages. |
| mart_repeat_customer | 94,401 | Repeat purchase flag and days active per customer. Measures true loyalty vs one-time buyers. |
| mart_product_price_range | 3,580 | Order volume by month × category × price bucket. Foundation for seasonal campaign planning. |
| mart_review_by_segment | 206 | Avg review score by HVC flag × payment type × state. Isolates which segments and conditions produce bad reviews. |

---

## Business Metric Definitions

| Metric | Definition | Where computed |
|---|---|---|
| High-value customer | `total_spend >= quantile(0.75)` across all customers | `mart_customer_payment_profile.is_high_value` |
| HVC threshold | R$ 180.52 (75th percentile of total spend) | Computed dynamically each run |
| Leakage score | `(national_avg - local_avg) / national_avg × order_count` | `mart_payment_leakage.leakage_score` |
| Installment bucket | `1` = single payment, `2-6` = low installments, `7-12` = medium, `12+` = high | `mart_customer_payment_profile.installment_bucket` |
| Revenue concentration | % of total revenue from HVC | 59.4% (R$ 9.27M / R$ 15.6M) |
| Repeat customer | Customer with more than 1 delivered/shipped/approved order | `mart_repeat_customer.is_repeat_customer` |

---

## Data Quality Issues Found in Source Dataset

The Olist dataset contains several data quality issues that required fixes in the pipeline. Each fix is applied at the raw load step before any data reaches ClickHouse.

| # | File(s) | Problem | Impact if unfixed | Fix applied |
|---|---|---|---|---|
| 1 | customers, sellers, geolocation | ZIP code columns (e.g. `01310`) read by pandas as `int64`, becoming `1310` and losing leading zeros. ClickHouse schema declares these as `String`. | `clickhouse-connect` crashes calling `len()` on an integer when building the binary insert block. | `dtype=str` passed to `pd.read_csv()` for affected columns |
| 2 | products.csv | Two column names contain a typo in the original dataset: `product_name_lenght` and `product_description_lenght` (missing the 'h'). ClickHouse schema has the correct spellings. | `ProgrammingError: Unrecognized column` — insert rejected entirely. | Rename columns after read using a `col_renames` dictionary |
| 3 | mql.csv | `origin` column has 60 null values. Schema declares it `String` (non-nullable). pandas represents nulls as `float('nan')`. | `TypeError: 'float' object has no attribute 'encode'` during insert. | `fillna('').astype(str)` applied to all object-dtype columns before insert |
| 4 | closed_deals.csv | `has_company` and `has_gtin` contain Python `True`/`False` booleans mixed with NaN. Stored as `object` dtype (not `bool`), so dtype filtering misses them. ClickHouse expects `Nullable(String)`. | Same `len()` crash as issue 1 — booleans have no length attribute. | Same `fillna('').astype(str)` fix converts booleans to `'True'`/`'False'` strings |
| 5 | Transform outputs | `max_installments_used` (UInt8) and `num_items` (UInt8) have NaN values produced by left joins where some orders have no matching payment or item records. ClickHouse non-nullable integer columns reject None. | `Unable to create Python array` — insert rejected. | `fillna(0).astype(int)` on affected columns before mart insert |
| 6 | Pipeline (architectural) | Metabase 0.49.0's ClickHouse community driver requires version 1.4.0 exactly — any other version fails silently or errors. | Metabase cannot connect to ClickHouse for dashboard queries. | Mirror all mart tables to PostgreSQL after each run. Metabase uses its native PostgreSQL connector with zero plugin dependency. |
| 7 | DAG definition | Airflow does not support `list >> list` syntax for task dependencies (`[a, b] >> [c, d]` raises `TypeError: unsupported operand type(s) for >>: 'list' and 'list'`). | DAG fails to parse — entire pipeline broken. | Rewrite as sequential dependency statements |
| 8 | ClickHouse queries | Complex multi-table JOIN queries through clickhouse-connect returned DataFrames with missing or unexpected column names in some cases. | `KeyError` on column names during pandas aggregation. | Break complex JOIN queries into separate simple queries, join in pandas |

---

## Processed Data

Pre-computed mart tables are exported to `data/processed/` as CSVs after each pipeline run. Usable in Python, R, or Excel without running the full stack.

```
data/processed/
  mart_customer_payment_profile.csv     94,401 rows
  mart_order_payment_detail.csv         99,441 rows
  mart_geo_payment_summary.csv          27 rows
  mart_payment_leakage.csv              107 rows
  mart_monthly_revenue.csv              88 rows
  mart_hv_category_preference.csv       25,187 rows
  mart_seller_acquisition.csv           842 rows
  mart_seller_performance.csv           2,978 rows
  mart_repeat_customer.csv              94,401 rows
  mart_product_price_range.csv          3,580 rows
  mart_review_by_segment.csv            206 rows
```

---

## Stopping the Stack

```bash
make down    # Stop containers, keep data
make clean   # Stop containers and delete all volumes (irreversible)
```

---

## Credentials

| Service    | URL                   | Username | Password    |
|------------|-----------------------|----------|-------------|
| Airflow    | http://localhost:8080 | admin    | admin       |
| Metabase   | http://localhost:3000 | —        | —           |
| ClickHouse | http://localhost:8123 | default  | dustinia123 |
| PostgreSQL | localhost:5432        | airflow  | airflow     |

Phase 0 — setup_schema
Runs first every time. Creates all ClickHouse tables with IF NOT EXISTS so fresh installs get the full schema. Also runs ALTER TABLE ADD COLUMN IF NOT EXISTS for columns added after the initial setup — this handles existing installations without requiring a database reset.

Phase 1 — extract_raw_data
Just a file existence check. Validates all 11 CSVs are in /opt/airflow/data/raw/ before anything else runs. Prevents partial pipeline execution if someone forgot to add a file.

Phase 2 — validate_data
Basic assertions on the 3 most critical files. Checks row counts are nonzero, primary keys have no nulls, payment values are non-negative. Pushes counts to Airflow XCom (a key-value store between tasks).

Phase 3 — load_raw_to_clickhouse
Reads each CSV with pandas, applies 4 source data fixes (see problems below), TRUNCATEs then INSERTs each of 11 raw tables. TRUNCATE before insert makes every re-run idempotent — no duplicate data.

Phases 4a–4j — 10 parallel transforms
Each reads from raw tables in ClickHouse, runs pandas aggregations and joins, and writes to a mart table. TRUNCATE before insert applies here too. They run in parallel where dependencies allow — 4g (seller performance) and 4i (price range) run directly after raw load, while 4e (HV categories) waits for both 4a and 4b.

Phase 5 — compute_leakage_scores
Runs after 4b (order detail) and 4c (geo summary). Calculates national average order value, groups by state × payment_type, computes leakage score, clips negative values (only underperformers are flagged).

Phase 6 — export_marts_to_postgres
Runs last, after all 10 transforms. Reads all 11 mart tables from ClickHouse, creates the dustinia schema in PostgreSQL if it doesn't exist, writes each mart with if_exists='replace'. This is the Metabase bridge — PostgreSQL is used because Metabase's ClickHouse driver is version-locked to specific Metabase releases, making it fragile. PostgreSQL is Metabase's native connector.
