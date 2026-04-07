# SCD Type 2 & Data Modelling — E-Commerce Analytics Pipeline

## 1. Apa itu SCD Type 2?

**Slowly Changing Dimension (SCD) Type 2** adalah teknik Data Warehouse untuk
melacak perubahan historis pada data dimensi.

| Pendekatan | SCD Type 1 | SCD Type 2 (Digunakan) |
|------------|-----------|------------------------|
| Strategi   | Overwrite data lama | Tambah baris baru, simpan histori |
| Histori    | Tidak tersimpan | Semua versi tersimpan |
| Storage    | Hemat | Bertumbuh seiring waktu |
| Query historis | Tidak bisa | "Status customer PADA SAAT transaksi?" |
| Use case ini | mart_sales_summary | dim_customer_scd2 |

---

## 2. Star Schema Design

```
                    ┌─────────────┐
                    │  dim_date   │
                    │  (date_key) │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴──────────────┐    ┌─────────────────┐
│  dim_browser │    │  fact_transactions  │    │  dim_payment    │
│ (browser_key)│◄───│  (central fact)     │───►│ (payment_key)   │
└──────────────┘    │                     │    └─────────────────┘
                    │  - transaction_id   │
┌──────────────┐    │  - purchase_price   │    ┌─────────────────┐
│dim_customer  │    │  - fraud flags (4)  │    │  dim_location   │
│(customer_key)│◄───│  - total_flags      │───►│ (location_key)  │
│  [SCD Type 2]│    │  - source, batch_id │    └─────────────────┘
└──────────────┘    └─────────────────────┘
```

---

## 3. dim_customer — SCD Type 2 Columns

| Kolom | SCD Type | Keterangan |
|-------|----------|------------|
| customer_id / email | **Type 0** | Fixed — tidak pernah berubah |
| job_title, company | **Type 1** | Overwrite — koreksi data saja |
| customer_segment | **Type 2** | Track history — VIP/Regular/Occasional/One-time |
| spending_tier | **Type 2** | Track history — Premium/High/Medium/Low |
| fraud_risk_score | **Type 2** | Track history — perubahan > 5 poin |
| fraud_risk_category | **Type 2** | Track history — Critical/High/Medium/Low/Clean |
| total_transactions | **Type 2** | Track history — akumulasi per run |
| total_spend | **Type 2** | Track history — akumulasi per run |
| effective_date | SCD Meta | Kapan versi ini mulai berlaku |
| expiry_date | SCD Meta | NULL = versi aktif; terisi = versi lama |
| is_current | SCD Meta | TRUE = aktif, FALSE = historis |
| version | SCD Meta | 1, 2, 3, ... bertambah tiap perubahan |
| change_reason | SCD Meta | 'initial_load', 'segment_upgrade', dll |

---

## 4. Contoh Data SCD Type 2

Pelanggan `john.doe@email.com` melakukan transaksi dari waktu ke waktu:

```
customer_key | customer_id          | segment    | tier    | fraud_score | eff_date   | exp_date   | is_curr | ver
-------------|----------------------|------------|---------|-------------|------------|------------|---------|----
1            | john.doe@email.com   | One-time   | Low     | 0           | 2024-01-10 | 2024-03-15 | FALSE   | 1
2            | john.doe@email.com   | Occasional | Medium  | 10          | 2024-03-15 | 2024-08-20 | FALSE   | 2
3            | john.doe@email.com   | Occasional | High    | 10          | 2024-08-20 | NULL       | TRUE    | 3
```

**Penjelasan:**
- **Versi 1:** Customer melakukan 1 transaksi → One-time, harga rendah → Low tier
- **Versi 2:** Transaksi ke-2 → Occasional segment, harga naik → Medium tier (BARIS BARU)
- **Versi 3:** Masih Occasional tapi spending naik → High tier (BARIS BARU LAGI)

---

## 5. Query Patterns

### 5.1 — Get Current Customer Profile
```sql
-- Profil customer terbaru (saat ini)
SELECT *
FROM dw.dim_customer
WHERE customer_id = 'john.doe@email.com'
  AND is_current = TRUE;

-- Atau gunakan view helper:
SELECT *
FROM dw.v_current_customers
WHERE customer_id = 'john.doe@email.com';
```

### 5.2 — Get Full Customer History (SCD2)
```sql
-- Semua versi customer dari awal
SELECT
    customer_id,
    version,
    customer_segment,
    spending_tier,
    fraud_risk_score,
    total_transactions,
    effective_date,
    expiry_date,
    is_current,
    change_reason
FROM dw.dim_customer
WHERE customer_id = 'john.doe@email.com'
ORDER BY version ASC;

-- Atau gunakan view:
SELECT * FROM dw.v_customer_history
WHERE customer_id = 'john.doe@email.com';
```

### 5.3 — Revenue by Segment (CORRECT — uses SCD2)
```sql
-- Revenue per customer segment dengan SCD2:
-- customer_key di fact menunjuk ke versi yang AKTIF SAAT TRANSAKSI TERJADI
-- bukan versi sekarang → akurat secara historis!

SELECT
    c.customer_segment,
    COUNT(f.transaction_key)      AS total_transactions,
    SUM(f.purchase_price)         AS total_revenue,
    AVG(f.purchase_price)         AS avg_order_value,
    COUNT(DISTINCT c.customer_id) AS unique_customers
FROM dw.fact_transactions f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;
```

### 5.4 — Revenue Trend by Month
```sql
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    SUM(f.purchase_price)   AS monthly_revenue,
    COUNT(*)                AS transaction_count,
    AVG(f.purchase_price)   AS avg_order_value
FROM dw.fact_transactions f
JOIN dw.dim_date dd ON f.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;
```

### 5.5 — Fraud Analysis by CC Provider
```sql
SELECT
    dp.cc_provider,
    COUNT(*)                                   AS total_transactions,
    SUM(f.purchase_price)                      AS total_revenue,
    SUM(f.total_fraud_flags)                   AS total_fraud_flags,
    AVG(f.total_fraud_flags)                   AS avg_flags_per_tx,
    COUNT(*) FILTER (WHERE f.flag_expired_cc)  AS expired_cc_count,
    COUNT(*) FILTER (WHERE f.flag_high_value)  AS high_value_count,
    ROUND(
        COUNT(*) FILTER (WHERE f.is_fraud_flagged) * 100.0 / COUNT(*),
        2
    )                                          AS fraud_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_payment dp ON f.payment_key = dp.payment_key
GROUP BY dp.cc_provider
ORDER BY total_revenue DESC;
```

### 5.6 — Segment Migration Report (SCD2 insight)
```sql
-- How many customers changed segment over time?
SELECT
    old_v.customer_segment  AS from_segment,
    new_v.customer_segment  AS to_segment,
    COUNT(*)                AS customer_count
FROM dw.dim_customer new_v
JOIN dw.dim_customer old_v
    ON new_v.customer_id = old_v.customer_id
    AND new_v.version = old_v.version + 1
WHERE new_v.customer_segment IS DISTINCT FROM old_v.customer_segment
GROUP BY old_v.customer_segment, new_v.customer_segment
ORDER BY customer_count DESC;
```

### 5.7 — Browser Performance Analysis
```sql
SELECT
    db.browser_name,
    db.os_type,
    COUNT(f.transaction_key)  AS total_transactions,
    SUM(f.purchase_price)     AS total_revenue,
    AVG(f.purchase_price)     AS avg_order_value,
    AVG(f.total_fraud_flags)  AS avg_fraud_flags
FROM dw.fact_transactions f
JOIN dw.dim_browser db ON f.browser_key = db.browser_key
GROUP BY db.browser_name, db.os_type
ORDER BY total_revenue DESC;
```

---

## 6. SCD Type 2 Update Process (via Airflow)

Setiap kali `ecommerce_batch_pipeline` DAG berjalan:

```
Step 1: dbt run --select dw.dim_payment     (Type 1 — upsert/overwrite)
Step 2: dbt run --select dw.dim_browser     (Type 0 — insert new only)
Step 3: dbt run --select dw.dim_customer_scd2  (Type 2 — incremental)
          └── detects Type 2 changes
          └── inserts new version rows for changed customers
          └── new customers get version 1
Step 4: UPDATE old versions expiry_date (via Airflow PythonOperator)
          └── SET expiry_date = NOW(), is_current = FALSE
          └── WHERE customer was in changed_customers list
Step 5: dbt run --select dw.fact_transactions  (incremental — new txns only)
          └── joins to current dim_customer version
          └── computes fraud flags
          └── appends new rows only
```

---

## 7. dbt Model Lineage — DW Layer

```
mart_customer_360 ──────────────────► dim_customer_scd2
                                            │
stg_purchases ──► dim_payment               │
stg_purchases ──► dim_browser               │
                                            │
stg_purchases ─────────────────────────────►┤
dim_payment ───────────────────────────────►│
dim_date (pre-generated) ──────────────────►│
dim_browser ───────────────────────────────►┘
                                            │
                                            ▼
                                    fact_transactions
```

---

## 8. Verifikasi SCD Type 2 di PostgreSQL

Setelah pipeline run, verifikasi di DBeaver atau psql:

```sql
-- Cek semua versi customer
SELECT customer_id, version, customer_segment, is_current, effective_date
FROM dw.dim_customer
ORDER BY customer_id, version;

-- Cek tidak ada customer dengan 2 current versions (data integrity)
SELECT customer_id, COUNT(*) AS current_versions
FROM dw.dim_customer
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows (setiap customer max 1 current version)

-- Total rows di fact_transactions
SELECT COUNT(*), SUM(purchase_price), AVG(total_fraud_flags)
FROM dw.fact_transactions;
-- Expected: 10,000 rows, ~$503,473 total, ~0.3 avg flags
```
