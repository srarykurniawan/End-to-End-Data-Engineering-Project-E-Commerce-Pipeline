# Technical Requirements — E-Commerce Batch Analytics Pipeline (v2)

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                    BATCH PIPELINE ARCHITECTURE                    │
│                                                                  │
│  CSV File (10K rows)                                             │
│      │                                                           │
│      ▼  [batch_ingest.py]                                        │
│  raw.ecommerce_purchases                                         │
│      │                                                           │
│      ▼  [data_quality_runner.py — 30 DQ Rules]                  │
│  quality.dq_results + quality.dq_run_summary                    │
│      │                                                           │
│      ▼  [dbt run — 9 models]                                     │
│  staging → intermediate → marts → dw (SCD2)                     │
│      │                                                           │
│      ▼  [Metabase]                                               │
│  Business Intelligence Dashboard                                 │
│                                                                  │
│  Orchestration: Apache Airflow (daily 01:00 WIB)                │
└──────────────────────────────────────────────────────────────────┘
```

**Revisi v2:**
- ❌ Dihapus: Kafka, Zookeeper, Spark, Grafana, kafka_producer.py, kafka_consumer.py
- ✅ Ditambah: Metabase (BI tool), quality schema (30 DQ rules), data_quality_runner.py

---

## 2. Technology Stack

| Layer | Teknologi | Versi | Fungsi | Alasan |
|-------|-----------|-------|--------|--------|
| **Ingestion** | Python + psycopg2 | Python 3.13 | Batch CSV loader | Simple, reliable, no overhead |
| **Storage** | PostgreSQL | 15 | Data warehouse | ACID, schema isolation, dbt native |
| **Transform** | dbt-core + dbt-postgres | 1.8.9 / 1.8.2 | SQL transformation | Modular, version control, auto-test |
| **Quality** | Python + SQL (custom) | — | 30 DQ rules engine | Full control, catalog-driven |
| **Orchestration** | Apache Airflow | 2.8.1 | DAG scheduling | DAG-based, rich UI, retry logic |
| **Visualization** | Metabase | Latest | BI Dashboard | Open source, SQL-friendly, no-code |
| **Infrastructure** | Docker + Compose | Latest | Containerization | Single-command deploy |

---

## 3. Infrastructure Requirements

### 3.1 Docker Services (Simplified dari v1)

| Service | Docker Image | Port (Host:Container) | RAM Min | CPU Min |
|---------|-------------|----------------------|---------|---------|
| PostgreSQL | postgres:15 | 5433:5432 | 512 MB | 0.5 core |
| Airflow Webserver | apache/airflow:2.8.1 | 8080:8080 | 1 GB | 1 core |
| Airflow Scheduler | apache/airflow:2.8.1 | — | 512 MB | 0.5 core |
| Airflow Init | apache/airflow:2.8.1 | — | 256 MB | 0.25 core |
| Metabase | metabase/metabase:latest | 3000:3000 | 1 GB | 1 core |
| **Total** | | | **~3.5 GB** | **~3.25 cores** |

> **v1 punya 10 services (7 GB RAM), v2 hanya 5 services (3.5 GB RAM) → lebih ringan 50%**

### 3.2 Database Schema Architecture

| Schema | Layer | Tables | Purpose |
|--------|-------|--------|---------|
| `raw` | Bronze | ecommerce_purchases | Data mentah append-only |
| `staging` | Silver | stg_purchases, int_customer_stats | Cleaned & aggregated |
| `marts` | Gold | mart_sales_summary, mart_customer_360, mart_fraud_signals | BI-ready |
| `dw` | DW | dim_customer_scd2, dim_payment, dim_browser, dim_date, fact_transactions | Star Schema + SCD2 |
| `quality` | Quality | dq_rules, dq_results, dq_run_summary, dq_anomalies | DQ tracking |
| `monitoring` | Ops | pipeline_runs | Execution log |

---

## 4. Data Quality Requirements

### 4.1 DQ Rule Categories (30 Rules Total)

| Category | Count | Description |
|----------|-------|-------------|
| Completeness | 7 | Kolom wajib tidak boleh NULL |
| Validity | 9 | Format dan range data harus valid |
| Uniqueness | 3 | PK/NK tidak boleh duplikat |
| Consistency | 6 | Relasi antar tabel harus konsisten |
| Freshness | 2 | Data harus fresh (< 24 jam) |
| Accuracy | 3 | Nilai harus sesuai business rules |

### 4.2 Severity Levels

| Severity | Action | Example |
|----------|--------|---------|
| CRITICAL | Pipeline STOP, alert | email IS NULL, purchase_id duplikat |
| HIGH | Log failure, continue | CC provider tidak valid |
| MEDIUM | Warning saja | Bahasa tidak dikenal |
| LOW | Info saja | IP format tidak sempurna |

### 4.3 DQ Health Score Formula

```
Health Score = (Rules PASS / Total Rules) × 100

< 70%  → Pipeline FAIL (critical threshold)
70-85% → Pipeline WARN
> 85%  → Pipeline PASS ✅
```

---

## 5. Airflow DAG Requirements

### DAG 1: ecommerce_batch_pipeline

```
Schedule: 0 18 * * * (= 01:00 WIB)
Max retries: 2 (delay 5 menit)
SLA: < 20 menit total

Flow:
start
  → ingest_csv_to_postgres     (PythonOp: batch_ingest.py)
  → run_data_quality            (PythonOp: data_quality_runner.py — 30 rules)
  → dbt_run_staging             (BashOp: dbt run staging intermediate)
  → dbt_run_marts               (BashOp: dbt run marts)
  → dbt_run_dw                  (BashOp: dbt run dw)
  → expire_scd2                 (PythonOp: fix SCD2 is_current flags)
  → dbt_test                    (BashOp: dbt test)
  → notify_complete             (PythonOp: log summary)
end
```

### DAG 2: ecommerce_data_quality_monitor

```
Schedule: 0 */6 * * * (= Setiap 6 jam)
Max retries: 1

Flow:
start → run_full_dq_checks → detect_anomalies → end
```

---

## 6. dbt Requirements

### Model Layers

| Model | Materialization | Source | Output |
|-------|----------------|--------|--------|
| stg_purchases | view | raw.ecommerce_purchases | Cleaned, parsed, validated |
| int_customer_stats | table | stg_purchases | Per-customer aggregation |
| mart_sales_summary | table | stg_purchases | Revenue per CC provider |
| mart_customer_360 | table | int_customer_stats | Segmentation + fraud score |
| mart_fraud_signals | table | stg + int | 4 fraud flags per transaction |
| dim_customer_scd2 | incremental | mart_customer_360 | SCD Type 2 history |
| dim_payment | table | stg_purchases | CC payment dimension |
| dim_browser | table | stg_purchases | Browser/OS dimension |
| fact_transactions | table | stg + all dims | Central fact table |

### dbt Tests (minimum per model)
- `not_null` pada semua PK kolom
- `unique` pada semua PK kolom
- `accepted_values` untuk categorical fields
- `relationships` untuk FK → PK

---

## 7. Metabase Requirements

### Connection Setup
```
Host: postgres (Docker internal)
Port: 5432
Database: ecommerce_db
User: postgres
Password: postgres
```

### Required Dashboards

| Dashboard | Source | Charts |
|-----------|--------|--------|
| Revenue Analytics | marts.mart_sales_summary | Bar (revenue by provider), Pie (market share), KPI |
| Customer Insights | marts.mart_customer_360 | Pie (segments), Table (top customers) |
| Fraud Detection | marts.mart_fraud_signals | Table (high risk), Bar (flags distribution) |
| Data Quality | quality.dq_results + dq_run_summary | Line (health trend), Table (failures) |
| Pipeline Health | monitoring.pipeline_runs | Table (run history), KPI (success rate) |

---

## 8. Performance SLA

| Requirement | Target | Notes |
|------------|--------|-------|
| Batch ingest 10K rows | < 30 detik | |
| DQ 30 rules execution | < 60 detik | |
| dbt run 9 models | < 60 detik | |
| dbt test | < 30 detik | |
| Total pipeline duration | < 20 menit | termasuk retry |
| Metabase query response | < 5 detik | per dashboard panel |
| Pipeline uptime | > 99% | per bulan |

---

## 9. Security Requirements

- Semua credentials di `.env` file (tidak di-commit ke Git)
- PostgreSQL: MD5 authentication
- Airflow Fernet Key untuk enkripsi connections
- `.gitignore` wajib exclude: `.env`, `logs/`, `__pycache__/`
- Metabase internal DB menggunakan database `metabase` terpisah

---

## 10. Port Reference

| Service | Host Access | Docker Internal |
|---------|------------|----------------|
| PostgreSQL | localhost:**5433** | postgres:**5432** |
| Airflow UI | localhost:8080 | airflow-webserver:8080 |
| Metabase | localhost:3000 | metabase:3000 |
