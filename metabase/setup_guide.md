# Metabase Setup Guide — E-Commerce Analytics Pipeline

## 1. Akses Metabase
```
URL   : http://localhost:3000
Waktu : Tunggu 2-3 menit setelah docker-compose up
```

## 2. Initial Setup (Pertama Kali)
1. Buka http://localhost:3000
2. Klik **Let's get started**
3. Isi nama, email, password admin
4. **Add your data** → pilih **PostgreSQL**
5. Isi connection:
   - Display name: `E-Commerce DW`
   - Host: `postgres`
   - Port: `5432`
   - Database name: `ecommerce_db`
   - Username: `postgres`
   - Password: `postgres`
6. Klik **Save**

## 3. Buat Dashboard: Revenue Analytics

### Panel 1 — Total Revenue (KPI)
```sql
SELECT ROUND(SUM(purchase_price)::numeric, 2) AS total_revenue
FROM staging.stg_purchases
```
Chart type: **Number**

### Panel 2 — Revenue by CC Provider (Bar)
```sql
SELECT cc_provider, total_revenue, revenue_share_pct
FROM marts.mart_sales_summary
ORDER BY total_revenue DESC
```
Chart type: **Bar**, X=cc_provider, Y=total_revenue

### Panel 3 — Customer Segments (Pie)
```sql
SELECT customer_segment, COUNT(*) AS jumlah
FROM marts.mart_customer_360
GROUP BY customer_segment
ORDER BY jumlah DESC
```
Chart type: **Pie**

### Panel 4 — Top 10 Fraud Transactions (Table)
```sql
SELECT email, ROUND(purchase_price::numeric,2) AS price, cc_provider,
       flag_high_value, flag_expired_cc, flag_abnormal_cvv,
       flag_high_risk_customer, total_flags
FROM marts.mart_fraud_signals
WHERE total_flags > 0
ORDER BY total_flags DESC, purchase_price DESC
LIMIT 10
```
Chart type: **Table**

## 4. Buat Dashboard: Data Quality Monitor

### Panel 1 — DQ Health Score (KPI)
```sql
SELECT ROUND(health_score::numeric, 1) AS health_score_pct
FROM quality.dq_run_summary
ORDER BY started_at DESC
LIMIT 1
```

### Panel 2 — Rules by Status (Bar)
```sql
SELECT status, COUNT(*) AS total_rules
FROM quality.dq_results
WHERE run_id = (SELECT run_id FROM quality.dq_run_summary ORDER BY started_at DESC LIMIT 1)
GROUP BY status
ORDER BY total_rules DESC
```

### Panel 3 — Failed Rules Detail (Table)
```sql
SELECT rule_name, rule_category, severity, failed_rows, failure_pct
FROM quality.dq_results
WHERE run_id = (SELECT run_id FROM quality.dq_run_summary ORDER BY started_at DESC LIMIT 1)
  AND status = 'FAIL'
ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END
```

### Panel 4 — DQ Health Trend (Line)
```sql
SELECT DATE(started_at) AS tanggal, ROUND(AVG(health_score)::numeric, 1) AS avg_health
FROM quality.dq_run_summary
GROUP BY DATE(started_at)
ORDER BY tanggal
```

### Panel 5 — Rules by Category (Bar)
```sql
SELECT rule_category, 
       COUNT(*) AS total,
       SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) AS passed,
       SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) AS failed
FROM quality.dq_results
WHERE run_id = (SELECT run_id FROM quality.dq_run_summary ORDER BY started_at DESC LIMIT 1)
GROUP BY rule_category
ORDER BY total DESC
```

## 5. Buat Dashboard: Pipeline Health

### Panel 1 — Pipeline Run History (Table)
```sql
SELECT dag_id, task_id, status, records_written,
       ROUND(duration_secs::numeric, 2) AS duration_secs,
       TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI') AS run_time
FROM monitoring.pipeline_runs
ORDER BY created_at DESC
LIMIT 20
```

## 6. Tips Metabase
- Gunakan **Filter** untuk pilih tanggal range
- Set **Auto-refresh**: 1 jam untuk DQ dashboard
- **Share dashboard** dengan link publik untuk presentasi
