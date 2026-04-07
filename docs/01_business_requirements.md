# Business Requirements — E-Commerce Analytics Pipeline

## 1. Latar Belakang

Platform e-commerce menghasilkan ribuan transaksi per hari yang mengandung informasi
kritis tentang perilaku pelanggan, performa payment provider, dan potensi risiko keuangan.
Tanpa sistem pengolahan data yang terstruktur, informasi ini tidak dapat dimanfaatkan
untuk pengambilan keputusan bisnis secara tepat waktu.

**Dataset:** 10.000 transaksi | 14 kolom | sumber: Ecommerce_Purchases.csv

---

## 2. Business Objectives

| ID    | Objective                                              | Metric Sukses                                      | Prioritas |
|-------|--------------------------------------------------------|----------------------------------------------------|-----------|
| BO-01 | Visibilitas revenue per payment provider               | Dashboard revenue tersedia < 1 jam setelah EOD     | HIGH      |
| BO-02 | Segmentasi pelanggan untuk program loyalitas           | Identifikasi segmen dengan akurasi > 95%           | HIGH      |
| BO-03 | Deteksi dan mitigasi fraud secara proaktif             | Flag fraud < 5 detik setelah transaksi masuk       | CRITICAL  |
| BO-04 | Analisis pola perilaku pembelian                       | Insight browser, bahasa, waktu tersedia harian     | MEDIUM    |
| BO-05 | Kualitas dan keandalan pipeline                        | DQ score > 98%, pipeline uptime > 99%             | HIGH      |

---

## 3. Business Requirements Detail

### BR-01 — Revenue Analytics
Platform harus menyajikan analisis pendapatan komprehensif per payment provider.

- Total revenue dan jumlah transaksi per CC Provider
- Market share (%) setiap provider terhadap total revenue
- Average Order Value (AOV) per provider
- Distribusi transaksi berdasarkan price tier:
  - Premium: $75–$100
  - High:    $50–$75
  - Medium:  $25–$50
  - Low:     $0–$25
- Persentase transaksi dengan CC expired per provider

**Insight dari data:**
- VISA 16 digit: revenue tertinggi $85,529 (17.0%)
- JCB 16 digit: transaksi terbanyak 1,716 tx
- Total revenue: ~$503,473 dari 10,000 transaksi
- AOV keseluruhan: $50.35

---

### BR-02 — Customer Intelligence
Profil 360 derajat setiap pelanggan untuk mendukung personalisasi dan loyalitas.

- Segmentasi customer berdasarkan frekuensi transaksi:
  - **VIP:**       >= 10 transaksi
  - **Regular:**   4–9 transaksi
  - **Occasional:** 2–3 transaksi
  - **One-time:**  1 transaksi
- Customer Lifetime Value: total spend, avg spend, frekuensi
- Preferensi: browser favorit, bahasa, CC provider, waktu belanja (AM/PM)
- Riwayat perubahan segmen dari waktu ke waktu (**SCD Type 2**)
- Engagement level berdasarkan pola pembelian

**Insight dari data:**
- 9,908 (99.1%) pelanggan adalah One-time buyer
- 46 pelanggan Occasional (2–3 transaksi)
- 9,954 unique customers dari 10,000 transaksi

---

### BR-03 — Fraud Detection & Risk Management
Deteksi potensi fraud secara real-time dengan akurasi tinggi.

| Flag                  | Rule                                        | Count dari Data    |
|-----------------------|---------------------------------------------|--------------------|
| flag_high_value       | purchase_price > $75                        | ~2,564 transaksi   |
| flag_expired_cc       | CC Exp Date sudah lewat Maret 2026          | 9,526 (95.3%)      |
| flag_abnormal_cvv     | panjang CVV bukan 3 digit                   | 1,689 (16.9%)      |
| flag_high_risk_cust   | fraud_risk_score customer > 75              | tergantung scoring |

- Fraud Risk Score gabungan: 0–100
- Alert otomatis untuk transaksi dengan total_flags >= 2
- Kategorisasi: Critical (score>80), High (60–80), Medium (40–60), Low (20–40), Clean (<20)

---

### BR-04 — Behavioral Analytics
Insight perilaku dan preferensi pelanggan untuk optimasi platform.

- Distribusi browser: Mozilla 79.2% | Opera 20.8%
- Preferensi bahasa: 9 bahasa (de, ru, el, pt, en, fr, es, it, zh)
- Pola waktu transaksi: AM 49.3% | PM 50.7%
- Korelasi profesi (623 profesi unik) dengan nilai transaksi
- Analisis OS dari browser user-agent string

---

### BR-05 — Operational Excellence
Pipeline harus berjalan otomatis, andal, dan terpantau.

- Batch pipeline harian otomatis pukul **01:00 WIB**
- Data Quality monitoring **setiap jam** dengan alerting
- Log lengkap setiap eksekusi pipeline
- SLA: pipeline batch selesai dalam **< 15 menit**
- Data freshness: tersedia maksimal **2 jam** setelah hari berakhir

---

## 4. Stakeholder & Use Case

| Stakeholder      | Kebutuhan Utama                          | Report/Dashboard       | Frekuensi   |
|------------------|------------------------------------------|------------------------|-------------|
| Business Analyst | Revenue & market share per provider      | Sales Summary          | Harian      |
| Marketing Team   | Segmentasi customer untuk campaign       | Customer 360           | Mingguan    |
| Risk/Fraud Team  | Deteksi transaksi mencurigakan           | Fraud Signals          | Real-time   |
| Data Engineer    | Pipeline health & data quality           | Pipeline Monitoring    | Setiap jam  |
| Product Manager  | Behavioral analytics & UX insight        | Behavior Analytics     | Bulanan     |

---

## 5. Success Criteria

| Kriteria                          | Target       | Actual        |
|-----------------------------------|--------------|---------------|
| Batch ingest 10K rows             | < 30 detik   | 8.16 detik ✅ |
| dbt run 5 models                  | < 60 detik   | ~12 detik ✅  |
| dbt test pass rate                | 100%         | 48/48 ✅      |
| Airflow DAG duration              | < 15 menit   | ~7 menit ✅   |
| Pipeline uptime                   | > 99%        | 100% ✅       |
| Grafana dashboard load            | < 5 detik    | < 2 detik ✅  |
