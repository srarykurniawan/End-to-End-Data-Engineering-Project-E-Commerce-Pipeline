# Architecture Diagram — E-Commerce Analytics Pipeline

## 1. High-Level Architecture (Lambda Architecture)

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                           DATA SOURCES                                       ║
║         Ecommerce_Purchases.csv          Simulated Real-time Events          ║
╚══════════════════╤═══════════════════════════════════╤════════════════════════╝
                   │                                   │
          [batch_ingest.py]                  [kafka_producer.py]
                   │                                   │
                   ▼                                   ▼
╔══════════════════╧═══════════════╗   ╔═══════════════╧════════════════════════╗
║         BATCH LAYER              ║   ║           SPEED LAYER                  ║
║                                  ║   ║                                        ║
║  raw.ecommerce_purchases         ║   ║  Apache Kafka                          ║
║       │                          ║   ║  Topic: ecommerce.purchases            ║
║  [Airflow: ecommerce_batch_pipeline]  ║  (200 messages, 1 msg/sec)            ║
║       │                          ║   ║       │                                ║
║  dbt staging (stg_purchases)     ║   ║  [kafka_consumer_spark.py]             ║
║       │                          ║   ║       │                                ║
║  dbt intermediate                ║   ║  raw.kafka_purchase_events             ║
║       │                          ║   ║       │                                ║
║  dbt marts (3 tables)            ║   ║  Near real-time fraud flags            ║
║       │                          ║   ║                                        ║
╚══════╤═══════════════════════════╝   ╚═══════════════╤════════════════════════╝
       │                                               │
       ╚═══════════════════╤═══════════════════════════╝
                           │
╔══════════════════════════╧══════════════════════════════════════════════════╗
║                        SERVING LAYER                                        ║
║                                                                             ║
║   PostgreSQL (localhost:5433)                                               ║
║   ├── Schema: raw          → ecommerce_purchases, kafka_purchase_events     ║
║   ├── Schema: staging      → stg_purchases, int_customer_stats              ║
║   ├── Schema: marts        → mart_sales_summary, mart_customer_360,         ║
║   │                          mart_fraud_signals                             ║
║   ├── Schema: dw           → fact_transactions, dim_customer (SCD2),        ║
║   │                          dim_payment, dim_date, dim_browser,            ║
║   │                          dim_location                                   ║
║   └── Schema: monitoring   → pipeline_runs, dq_results                     ║
║                                                                             ║
║   Grafana (localhost:3000) → 12+ analytical panels                          ║
╚═════════════════════════════════════════════════════════════════════════════╝
```

---

## 2. Data Flow Detail

### 2.1 Batch Flow
```
CSV (10,000 rows)
    │
    ▼ batch_ingest.py
    │   - pandas read_csv
    │   - data cleaning & validation
    │   - executemany() batch insert (1,000 rows/batch)
    │   - log ke monitoring.pipeline_runs
    ▼
raw.ecommerce_purchases
    │
    ▼ Airflow triggers dbt
    │
    ├─► dbt: stg_purchases (VIEW)
    │         - parse cc_exp_date → month, year
    │         - extract browser_name, os_type dari user-agent
    │         - standardize language_code (lowercase)
    │         - generate purchase_id (MD5 surrogate key)
    │         - 48 automated data tests
    │
    ├─► dbt: int_customer_stats (TABLE)
    │         - GROUP BY email
    │         - total_transactions, total_spend, avg_spend
    │         - preferred_browser, os, cc_provider, language
    │         - has_expired_cc, unique_cards_used
    │
    ├─► dbt: mart_sales_summary (TABLE)
    │         - revenue per CC provider
    │         - market share %, AOV, expired_cc_pct
    │         - premium/high/medium/low count
    │
    ├─► dbt: mart_customer_360 (TABLE)
    │         - customer segmentation (VIP/Regular/Occasional/One-time)
    │         - fraud_risk_score (0-100)
    │         - spending_tier, engagement_level
    │
    ├─► dbt: mart_fraud_signals (TABLE)
    │         - 4 fraud flags per transaksi
    │         - flag_high_value, flag_expired_cc
    │         - flag_abnormal_cvv, flag_high_risk_customer
    │         - total_flags (0-4)
    │
    └─► dbt: DW layer (SCD Type 2)
              - dim_customer_scd2 (track segment changes)
              - dim_payment (CC info)
              - dim_date (calendar dimension)
              - dim_browser (browser/OS info)
              - fact_transactions (grain: 1 row per transaction)
```

### 2.2 Streaming Flow
```
CSV Row
    │
    ▼ kafka_producer.py
    │   - send 1 message per row
    │   - enrich dengan event_id (UUID), timestamp, schema_version
    │   - api_version=(2,5,0) untuk skip version probe
    │   - advertise: 127.0.0.1:9092 (hindari IPv6 issue Windows)
    ▼
Kafka Broker (Topic: ecommerce.purchases)
    │   - 3 partitions
    │   - retention: 7 days
    ▼
kafka_consumer_spark.py (Spark Structured Streaming)
    │   - consume messages
    │   - parse JSON schema
    │   - apply fraud detection rules
    ▼
raw.kafka_purchase_events (PostgreSQL)
    │
    ▼
Near real-time fraud signals
```

---

## 3. Component Architecture

### 3.1 Docker Services Network

```
┌─────────────────────────────────────────────────────────────┐
│                   pipeline-network (bridge)                  │
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌───────────────────────┐  │
│  │Zookeeper │───►│  Kafka   │◄───│     Kafka UI          │  │
│  │  :2181   │    │  :9092   │    │     :8090             │  │
│  └──────────┘    └──────────┘    └───────────────────────┘  │
│                       │                                      │
│                  [kafka_producer]                            │
│                  [kafka_consumer]                            │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐                       │
│  │ Spark Master │───►│ Spark Worker │                       │
│  │    :8081     │    │              │                       │
│  └──────────────┘    └──────────────┘                       │
│                                                             │
│  ┌──────────────────────────────────┐                       │
│  │          PostgreSQL :5432        │                       │
│  │  (exposed as :5433 ke host)      │                       │
│  └──────────────────────────────────┘                       │
│           ▲             ▲        ▲                          │
│           │             │        │                          │
│  ┌────────┴───┐  ┌──────┴──┐  ┌──┴──────┐                 │
│  │  Airflow   │  │   dbt   │  │ Grafana │                 │
│  │ Webserver  │  │  (CLI)  │  │  :3000  │                 │
│  │  :8080     │  └─────────┘  └─────────┘                 │
│  ├────────────┤                                            │
│  │  Airflow   │                                            │
│  │ Scheduler  │                                            │
│  └────────────┘                                            │
└─────────────────────────────────────────────────────────────┘

Host Machine (Windows)
├── localhost:5433  → PostgreSQL
├── localhost:8080  → Airflow UI
├── localhost:3000  → Grafana
├── localhost:8090  → Kafka UI
└── localhost:8081  → Spark Master UI
```

### 3.2 Airflow DAG Architecture

```
DAG: ecommerce_batch_pipeline
Schedule: 0 1 * * * (01:00 WIB daily)
─────────────────────────────────────────────────────────
start (DummyOperator)
  │
  ▼
ingest_csv_to_postgres (PythonOperator)
  │  - run batch_ingest.py
  │  - log records_written ke monitoring
  │
  ▼
data_quality_raw (PythonOperator)
  │  - row count > 0
  │  - null check pada kolom kritis
  │  - negative price check
  │  - log ke monitoring.dq_results
  │
  ▼
dbt_run_staging (BashOperator)
  │  - dbt run --select staging intermediate
  │  - env: POSTGRES_HOST=postgres, PORT=5432
  │
  ▼
dbt_run_marts (BashOperator)
  │  - dbt run --select marts
  │
  ▼
dbt_run_dw (BashOperator)              ← NEW
  │  - dbt run --select dw
  │  - SCD Type 2 upsert dim_customer
  │  - populate fact_transactions
  │
  ▼
dbt_test (BashOperator)
  │  - dbt test (48+ tests)
  │
  ▼
notify_pipeline_complete (PythonOperator)
  │  - log status, duration, records ke monitoring
  │
  ▼
end (DummyOperator)
─────────────────────────────────────────────────────────

DAG: ecommerce_data_quality_monitor
Schedule: 0 * * * * (setiap jam)
─────────────────────────────────────────────────────────
check_data_freshness
  └─► check_volume_anomaly
        └─► check_marts_integrity
              └─► evaluate_health_score
                    └─► alert_or_log
```

---

## 4. dbt Model Lineage

```
Sources (raw schema)
└── raw.ecommerce_purchases
        │
        ▼
staging/stg_purchases (VIEW)
        │
        ├──────────────────────────────────►mart_sales_summary (TABLE)
        │                                       └── CC provider revenue analytics
        │
        ▼
intermediate/int_customer_stats (TABLE)
        │
        ├──────────────────────────────────►mart_customer_360 (TABLE)
        │                                       └── Customer segmentation + scoring
        │
        ├── (join with stg_purchases) ────►mart_fraud_signals (TABLE)
        │                                       └── Per-transaction fraud flags
        │
        └── (SCD Type 2 upsert) ─────────►dw/dim_customer_scd2 (INCREMENTAL)
                                               └── Historical customer profile tracking

stg_purchases (also feeds DW layer):
        ├────────────────────────────────►dw/dim_payment (TABLE)
        ├────────────────────────────────►dw/dim_browser (TABLE)
        └── (join all dims) ────────────►dw/fact_transactions (INCREMENTAL)
                                               └── Central fact table, Star Schema

dim_date (TABLE)  ←── generated from date range
        └── referenced by fact_transactions
```

---

## 5. Service URLs & Access

| Service       | URL                    | Credentials    | Keterangan                    |
|---------------|------------------------|----------------|-------------------------------|
| Airflow UI    | http://localhost:8080  | admin / admin  | DAG management & monitoring   |
| Grafana       | http://localhost:3000  | admin / admin  | Business intelligence dashboard |
| Kafka UI      | http://localhost:8090  | —              | Topic & message monitoring    |
| Spark UI      | http://localhost:8081  | —              | Job & worker monitoring       |
| PostgreSQL    | localhost:5433         | postgres/postgres | External connection (DBeaver) |
