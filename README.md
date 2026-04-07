# E-Commerce Batch Analytics Pipeline

End-to-end Data Engineering project — **Batch only**, Metabase visualization,
Enhanced Data Quality dengan 30 DQ rules.

## Perubahan dari v1
| Aspek | v1 | v2 |
|-------|----|----|
| Jalur data | Batch + Streaming (Kafka) | **Batch only** |
| Visualisasi | Grafana | **Metabase** |
| Data Quality | Basic (5 checks) | **30 DQ rules, 6 categories** |
| Services Docker | 10 (7 GB RAM) | **5 (3.5 GB RAM)** |
| DQ tracking | monitoring.dq_results | **quality schema (4 tables)** |

## Architecture

```
CSV File (10,000 rows)
    │
    ▼ batch_ingest.py (Python + psycopg2)
    │
raw.ecommerce_purchases
    │
    ▼ data_quality_runner.py (30 DQ rules)
    │
quality.dq_results + dq_run_summary
    │
    ▼ dbt run (9 models)
    │
staging → marts → dw (SCD Type 2)
    │
    ▼ Metabase Dashboard
Business Intelligence (5 dashboards)

Orchestration: Apache Airflow (daily 01:00 WIB)
```

## Quick Start

```powershell
# 1. Masuk ke folder project
cd ecommerce-pipeline-finalproject

# 2. Copy .env
cp .env.example .env

# 3. Jalankan Docker (hanya 5 services)
docker-compose up -d

# 4. Buat database airflow & metabase
docker exec postgres psql -U postgres -c "CREATE DATABASE airflow;"
docker exec postgres psql -U postgres -c "CREATE DATABASE metabase;"

# 5. Restart Airflow setelah DB siap
docker-compose restart airflow-init airflow-webserver airflow-scheduler

# 6. Run pipeline manual
bash scripts/run_pipeline.sh
```

## Service URLs

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | setup saat pertama buka |
| PostgreSQL | localhost:5433 | postgres / postgres |

## Data Quality

30 DQ rules dikategorikan menjadi 6:
- **Completeness** (7): Kolom wajib tidak NULL
- **Validity** (9): Format dan range benar
- **Uniqueness** (3): Tidak ada duplikat
- **Consistency** (6): Relasi antar tabel konsisten
- **Freshness** (2): Data tidak lebih dari 24 jam
- **Accuracy** (3): Nilai sesuai business rules

```powershell
# Run DQ manual
python scripts/data_quality_runner.py
```

## dbt

```powershell
cd dbt
dbt deps          # Install packages
dbt run           # Jalankan semua models
dbt test          # Jalankan semua tests
```

## Port Reference

| Dari | Host | Port |
|------|------|------|
| Windows / DBeaver | localhost | 5433 |
| Dalam Docker (Airflow/dbt) | postgres | 5432 |
| Metabase connect ke DB | postgres | 5432 |
