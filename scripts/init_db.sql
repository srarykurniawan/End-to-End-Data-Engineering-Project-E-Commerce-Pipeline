-- =============================================================================
-- E-Commerce Batch Analytics Pipeline — Complete Database Initialization v2
-- Batch Only (no Kafka) | Metabase Visualization | Enhanced Data Quality
-- =============================================================================

-- ── Create Databases ────────────────────────────────────────────────────────
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

SELECT 'CREATE DATABASE metabase' WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'metabase'
)\gexec

-- ── Create Schemas ───────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS quality;      -- Data Quality dedicated schema
CREATE SCHEMA IF NOT EXISTS monitoring;

-- =============================================================================
-- SCHEMA: raw (Bronze Layer)
-- =============================================================================
DROP TABLE IF EXISTS raw.ecommerce_purchases CASCADE;
CREATE TABLE raw.ecommerce_purchases (
    id                BIGSERIAL     PRIMARY KEY,
    address           TEXT,
    lot               VARCHAR(50),
    am_or_pm          VARCHAR(2)    CHECK (am_or_pm IN ('AM','PM')),
    browser_info      TEXT,
    company           VARCHAR(255),
    credit_card       VARCHAR(20),
    cc_exp_date       VARCHAR(7),
    cc_security_code  VARCHAR(10),
    cc_provider       VARCHAR(100),
    email             VARCHAR(255),
    job               VARCHAR(255),
    ip_address        VARCHAR(45),
    language          VARCHAR(10),
    purchase_price    NUMERIC(10,2),
    ingested_at       TIMESTAMP     NOT NULL DEFAULT NOW(),
    batch_id          VARCHAR(50),
    source            VARCHAR(50)   DEFAULT 'csv_batch'
);
CREATE INDEX idx_raw_ep_email       ON raw.ecommerce_purchases(email);
CREATE INDEX idx_raw_ep_ingested_at ON raw.ecommerce_purchases(ingested_at);
CREATE INDEX idx_raw_ep_batch_id    ON raw.ecommerce_purchases(batch_id);
COMMENT ON TABLE raw.ecommerce_purchases IS 'Bronze: raw CSV data, append-only';

-- =============================================================================
-- SCHEMA: quality (Data Quality Layer)
-- =============================================================================

-- DQ Rules definition table
DROP TABLE IF EXISTS quality.dq_rules CASCADE;
CREATE TABLE quality.dq_rules (
    rule_id           SERIAL        PRIMARY KEY,
    rule_name         VARCHAR(100)  NOT NULL UNIQUE,
    rule_category     VARCHAR(50)   NOT NULL, -- completeness, validity, uniqueness, consistency, freshness, accuracy
    target_schema     VARCHAR(50)   NOT NULL,
    target_table      VARCHAR(100)  NOT NULL,
    target_column     VARCHAR(100),
    rule_description  TEXT          NOT NULL,
    sql_check         TEXT          NOT NULL,  -- SQL that returns count of violations
    severity          VARCHAR(10)   NOT NULL DEFAULT 'HIGH' CHECK (severity IN ('CRITICAL','HIGH','MEDIUM','LOW')),
    threshold_pct     NUMERIC(5,2)  DEFAULT 0, -- max allowed violation %
    is_active         BOOLEAN       DEFAULT TRUE,
    created_at        TIMESTAMP     DEFAULT NOW()
);
COMMENT ON TABLE quality.dq_rules IS 'Catalog of all data quality rules with SQL checks';

-- DQ Run results
DROP TABLE IF EXISTS quality.dq_results CASCADE;
CREATE TABLE quality.dq_results (
    result_id         BIGSERIAL     PRIMARY KEY,
    run_id            VARCHAR(100)  NOT NULL,
    rule_id           INTEGER       REFERENCES quality.dq_rules(rule_id),
    rule_name         VARCHAR(100)  NOT NULL,
    rule_category     VARCHAR(50)   NOT NULL,
    target_table      VARCHAR(100)  NOT NULL,
    target_column     VARCHAR(100),
    total_rows        BIGINT        DEFAULT 0,
    failed_rows       BIGINT        DEFAULT 0,
    passed_rows       BIGINT        DEFAULT 0,
    failure_pct       NUMERIC(7,4)  DEFAULT 0,
    threshold_pct     NUMERIC(5,2)  DEFAULT 0,
    status            VARCHAR(10)   NOT NULL CHECK (status IN ('PASS','FAIL','WARN','ERROR')),
    severity          VARCHAR(10),
    error_message     TEXT,
    sample_failures   JSONB,        -- up to 5 sample failing rows
    checked_at        TIMESTAMP     DEFAULT NOW(),
    duration_ms       INTEGER       DEFAULT 0
);
CREATE INDEX idx_dq_results_run_id    ON quality.dq_results(run_id);
CREATE INDEX idx_dq_results_status    ON quality.dq_results(status);
CREATE INDEX idx_dq_results_category  ON quality.dq_results(rule_category);
CREATE INDEX idx_dq_results_checked   ON quality.dq_results(checked_at DESC);
COMMENT ON TABLE quality.dq_results IS 'Results of every DQ rule execution';

-- DQ Summary per run
DROP TABLE IF EXISTS quality.dq_run_summary CASCADE;
CREATE TABLE quality.dq_run_summary (
    run_id            VARCHAR(100)  PRIMARY KEY,
    dag_id            VARCHAR(100),
    total_rules       INTEGER       DEFAULT 0,
    passed_rules      INTEGER       DEFAULT 0,
    failed_rules      INTEGER       DEFAULT 0,
    warned_rules      INTEGER       DEFAULT 0,
    critical_failures INTEGER       DEFAULT 0,
    health_score      NUMERIC(5,2)  DEFAULT 0, -- 0-100
    overall_status    VARCHAR(10)   CHECK (overall_status IN ('PASS','FAIL','WARN')),
    started_at        TIMESTAMP     DEFAULT NOW(),
    completed_at      TIMESTAMP,
    duration_secs     NUMERIC(10,2)
);
COMMENT ON TABLE quality.dq_run_summary IS 'Aggregated DQ summary per pipeline run';

-- DQ anomaly tracking (trend over time)
DROP TABLE IF EXISTS quality.dq_anomalies CASCADE;
CREATE TABLE quality.dq_anomalies (
    anomaly_id        BIGSERIAL     PRIMARY KEY,
    run_id            VARCHAR(100),
    rule_name         VARCHAR(100),
    anomaly_type      VARCHAR(50),  -- sudden_spike, threshold_breach, new_nulls, etc.
    description       TEXT,
    detected_at       TIMESTAMP     DEFAULT NOW(),
    resolved_at       TIMESTAMP,
    is_resolved       BOOLEAN       DEFAULT FALSE
);
COMMENT ON TABLE quality.dq_anomalies IS 'Tracks DQ anomalies for trend analysis';

-- Insert DQ Rules
INSERT INTO quality.dq_rules
    (rule_name, rule_category, target_schema, target_table, target_column,
     rule_description, sql_check, severity, threshold_pct)
VALUES
-- ── COMPLETENESS RULES ─────────────────────────────────────────────────────
('raw_email_not_null', 'completeness', 'raw', 'ecommerce_purchases', 'email',
 'Email harus tidak boleh NULL di setiap transaksi',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE email IS NULL OR TRIM(email) = ''''',
 'CRITICAL', 0),

('raw_purchase_price_not_null', 'completeness', 'raw', 'ecommerce_purchases', 'purchase_price',
 'Purchase price tidak boleh NULL',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE purchase_price IS NULL',
 'CRITICAL', 0),

('raw_cc_provider_not_null', 'completeness', 'raw', 'ecommerce_purchases', 'cc_provider',
 'CC Provider tidak boleh NULL',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE cc_provider IS NULL OR TRIM(cc_provider) = ''''',
 'HIGH', 0),

('raw_credit_card_not_null', 'completeness', 'raw', 'ecommerce_purchases', 'credit_card',
 'Nomor kartu kredit tidak boleh NULL',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE credit_card IS NULL OR TRIM(credit_card) = ''''',
 'HIGH', 0),

('raw_cc_exp_date_not_null', 'completeness', 'raw', 'ecommerce_purchases', 'cc_exp_date',
 'Tanggal expired CC tidak boleh NULL',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE cc_exp_date IS NULL',
 'HIGH', 0),

('raw_ip_address_not_null', 'completeness', 'raw', 'ecommerce_purchases', 'ip_address',
 'IP Address tidak boleh NULL',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE ip_address IS NULL',
 'MEDIUM', 1),

('raw_language_not_null', 'completeness', 'raw', 'ecommerce_purchases', 'language',
 'Language code tidak boleh NULL',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE language IS NULL',
 'MEDIUM', 0.5),

-- ── VALIDITY RULES ─────────────────────────────────────────────────────────
('raw_purchase_price_positive', 'validity', 'raw', 'ecommerce_purchases', 'purchase_price',
 'Purchase price tidak boleh negatif',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE purchase_price < 0',
 'CRITICAL', 0),

('raw_purchase_price_range', 'validity', 'raw', 'ecommerce_purchases', 'purchase_price',
 'Purchase price harus dalam range valid $0.00-$9999.99',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE purchase_price > 9999.99',
 'HIGH', 0),

('raw_am_pm_valid', 'validity', 'raw', 'ecommerce_purchases', 'am_or_pm',
 'AM or PM hanya boleh bernilai AM atau PM',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE am_or_pm NOT IN (''AM'', ''PM'')',
 'HIGH', 0),

('raw_cc_exp_date_format', 'validity', 'raw', 'ecommerce_purchases', 'cc_exp_date',
 'Format CC Exp Date harus MM/YY (misal: 03/26)',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE cc_exp_date !~ ''^(0[1-9]|1[0-2])/[0-9]{2}$''',
 'HIGH', 0),

('raw_language_valid', 'validity', 'raw', 'ecommerce_purchases', 'language',
 'Language harus salah satu dari 9 bahasa yang valid',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE language NOT IN (''de'',''ru'',''el'',''pt'',''en'',''fr'',''es'',''it'',''zh'')',
 'MEDIUM', 0),

('raw_cc_provider_valid', 'validity', 'raw', 'ecommerce_purchases', 'cc_provider',
 'CC Provider harus salah satu dari 10 provider yang dikenal',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE cc_provider NOT IN (''JCB 16 digit'',''VISA 16 digit'',''JCB 15 digit'',''American Express'',''Maestro'',''Voyager'',''Discover'',''Mastercard'',''VISA 13 digit'',''Diners Club / Carte Blanche'')',
 'MEDIUM', 0),

('raw_email_format', 'validity', 'raw', 'ecommerce_purchases', 'email',
 'Format email harus valid (mengandung @ dan domain)',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE email !~ ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$''',
 'HIGH', 0.1),

('raw_cvv_length', 'validity', 'raw', 'ecommerce_purchases', 'cc_security_code',
 'CVV harus 3 atau 4 digit angka',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE LENGTH(TRIM(cc_security_code)) NOT IN (3, 4)',
 'MEDIUM', 20),

('raw_ip_format', 'validity', 'raw', 'ecommerce_purchases', 'ip_address',
 'Format IP address harus valid IPv4',
 'SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE ip_address !~ ''^([0-9]{1,3}\.){3}[0-9]{1,3}$''',
 'LOW', 1),

-- ── UNIQUENESS RULES ────────────────────────────────────────────────────────
('raw_no_exact_duplicate', 'uniqueness', 'raw', 'ecommerce_purchases', NULL,
 'Tidak boleh ada baris yang identik 100% (semua kolom sama)',
 'SELECT COUNT(*) - COUNT(DISTINCT (email, credit_card, purchase_price, ingested_at::date)) FROM raw.ecommerce_purchases',
 'HIGH', 0.1),

('stg_purchase_id_unique', 'uniqueness', 'staging', 'stg_purchases', 'purchase_id',
 'purchase_id (surrogate key) harus unik di staging',
 'SELECT COUNT(*) - COUNT(DISTINCT purchase_id) FROM staging.stg_purchases',
 'CRITICAL', 0),

('stg_customer_email_unique', 'uniqueness', 'staging', 'int_customer_stats', 'email',
 'Setiap email harus muncul tepat 1 kali di int_customer_stats',
 'SELECT COUNT(*) - COUNT(DISTINCT email) FROM staging.int_customer_stats',
 'CRITICAL', 0),

-- ── CONSISTENCY RULES ───────────────────────────────────────────────────────
('stg_fraud_flags_consistent', 'consistency', 'staging', 'stg_purchases', NULL,
 'total_flags di mart_fraud_signals harus konsisten (0-4)',
 'SELECT COUNT(*) FROM marts.mart_fraud_signals WHERE total_flags < 0 OR total_flags > 4',
 'HIGH', 0),

('stg_row_count_matches_raw', 'consistency', 'staging', 'stg_purchases', NULL,
 'Jumlah baris staging tidak boleh lebih dari raw (filter valid saja)',
 'SELECT CASE WHEN (SELECT COUNT(*) FROM staging.stg_purchases) > (SELECT COUNT(*) FROM raw.ecommerce_purchases) THEN 1 ELSE 0 END',
 'HIGH', 0),

('mart_revenue_share_100', 'consistency', 'marts', 'mart_sales_summary', 'revenue_share_pct',
 'Total revenue_share_pct di mart_sales_summary harus = 100%',
 'SELECT ABS(100 - ROUND(SUM(revenue_share_pct)::numeric, 0))::bigint FROM marts.mart_sales_summary',
 'HIGH', 0),

('mart_customer_segment_valid', 'consistency', 'marts', 'mart_customer_360', 'customer_segment',
 'customer_segment hanya boleh: VIP, Regular, Occasional, One-time',
 'SELECT COUNT(*) FROM marts.mart_customer_360 WHERE customer_segment NOT IN (''VIP'',''Regular'',''Occasional'',''One-time'')',
 'HIGH', 0),

('mart_fraud_score_range', 'consistency', 'marts', 'mart_customer_360', 'fraud_risk_score',
 'fraud_risk_score harus antara 0 dan 100',
 'SELECT COUNT(*) FROM marts.mart_customer_360 WHERE fraud_risk_score < 0 OR fraud_risk_score > 100',
 'HIGH', 0),

('dw_scd2_no_duplicate_current', 'consistency', 'dw', 'dim_customer_scd2', 'is_current',
 'Setiap customer harus punya tepat 1 versi is_current=TRUE di SCD2',
 'SELECT COUNT(*) FROM (SELECT customer_id FROM dw.dim_customer_scd2 WHERE is_current=TRUE GROUP BY customer_id HAVING COUNT(*)>1) x',
 'CRITICAL', 0),

-- ── FRESHNESS RULES ─────────────────────────────────────────────────────────
('raw_data_freshness_24h', 'freshness', 'raw', 'ecommerce_purchases', 'ingested_at',
 'Data raw harus diingest dalam 24 jam terakhir',
 'SELECT CASE WHEN EXTRACT(EPOCH FROM (NOW() - MAX(ingested_at)))/3600 > 25 THEN 1 ELSE 0 END FROM raw.ecommerce_purchases',
 'HIGH', 0),

('raw_min_row_count', 'freshness', 'raw', 'ecommerce_purchases', NULL,
 'Raw table harus memiliki minimal 100 baris data',
 'SELECT CASE WHEN COUNT(*) < 100 THEN 1 ELSE 0 END FROM raw.ecommerce_purchases',
 'HIGH', 0),

-- ── ACCURACY RULES ──────────────────────────────────────────────────────────
('stg_cc_exp_month_range', 'accuracy', 'staging', 'stg_purchases', 'cc_exp_month',
 'CC expiry month harus antara 1-12',
 'SELECT COUNT(*) FROM staging.stg_purchases WHERE cc_exp_month < 1 OR cc_exp_month > 12',
 'HIGH', 0),

('stg_cc_exp_year_range', 'accuracy', 'staging', 'stg_purchases', 'cc_exp_year',
 'CC expiry year harus antara 2000-2040',
 'SELECT COUNT(*) FROM staging.stg_purchases WHERE cc_exp_year < 2000 OR cc_exp_year > 2040',
 'HIGH', 0),

('stg_price_tier_accuracy', 'accuracy', 'staging', 'stg_purchases', 'price_tier',
 'Price tier harus sesuai dengan purchase_price range',
 'SELECT COUNT(*) FROM staging.stg_purchases WHERE (price_tier=''Low'' AND purchase_price >= 25) OR (price_tier=''Premium'' AND purchase_price < 75)',
 'MEDIUM', 0);

-- =============================================================================
-- SCHEMA: monitoring (Operational Layer)
-- =============================================================================
DROP TABLE IF EXISTS monitoring.pipeline_runs CASCADE;
CREATE TABLE monitoring.pipeline_runs (
    id               BIGSERIAL    PRIMARY KEY,
    run_id           VARCHAR(100) UNIQUE NOT NULL,
    dag_id           VARCHAR(100),
    task_id          VARCHAR(100),
    status           VARCHAR(20)  CHECK (status IN ('running','success','failed','skipped')),
    records_read     INTEGER      DEFAULT 0,
    records_written  INTEGER      DEFAULT 0,
    records_failed   INTEGER      DEFAULT 0,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    duration_secs    NUMERIC(10,2),
    error_message    TEXT,
    created_at       TIMESTAMP    DEFAULT NOW()
);

-- =============================================================================
-- SCHEMA: dw (Star Schema + SCD Type 2)
-- =============================================================================
DROP TABLE IF EXISTS dw.dim_date CASCADE;
CREATE TABLE dw.dim_date (
    date_key     INTEGER  PRIMARY KEY,
    full_date    DATE     NOT NULL UNIQUE,
    day_of_week  INTEGER, day_name VARCHAR(10), day_of_month INTEGER,
    day_of_year  INTEGER, week_of_year INTEGER, month INTEGER,
    month_name   VARCHAR(10), quarter INTEGER, year INTEGER,
    is_weekend   BOOLEAN  DEFAULT FALSE, is_holiday BOOLEAN DEFAULT FALSE
);

INSERT INTO dw.dim_date
SELECT TO_CHAR(d,'YYYYMMDD')::INTEGER, d::DATE,
    EXTRACT(ISODOW FROM d)::INTEGER, TO_CHAR(d,'Day'),
    EXTRACT(DAY FROM d)::INTEGER, EXTRACT(DOY FROM d)::INTEGER,
    EXTRACT(WEEK FROM d)::INTEGER, EXTRACT(MONTH FROM d)::INTEGER,
    TO_CHAR(d,'Month'), EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(YEAR FROM d)::INTEGER, EXTRACT(ISODOW FROM d) IN (6,7), FALSE
FROM generate_series('2015-01-01'::DATE,'2030-12-31'::DATE,'1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;

DROP TABLE IF EXISTS dw.dim_payment CASCADE;
CREATE TABLE dw.dim_payment (
    payment_key SERIAL PRIMARY KEY, credit_card_number VARCHAR(20) UNIQUE NOT NULL,
    cc_provider VARCHAR(100), cc_exp_date VARCHAR(7), cc_exp_month INTEGER,
    cc_exp_year INTEGER, is_expired BOOLEAN DEFAULT FALSE,
    cvv_length INTEGER, is_cvv_abnormal BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS dw.dim_browser CASCADE;
CREATE TABLE dw.dim_browser (
    browser_key SERIAL PRIMARY KEY, browser_name VARCHAR(50) NOT NULL,
    os_type VARCHAR(50), browser_family VARCHAR(50),
    UNIQUE (browser_name, os_type)
);

DROP TABLE IF EXISTS dw.dim_customer_scd2 CASCADE;
CREATE TABLE dw.dim_customer_scd2 (
    customer_key          BIGSERIAL    PRIMARY KEY,
    customer_id           VARCHAR(255) NOT NULL,
    email                 VARCHAR(255) NOT NULL,
    job_title             VARCHAR(255),
    company_name          VARCHAR(255),
    customer_segment      VARCHAR(20),
    spending_tier         VARCHAR(20),
    total_transactions    INTEGER      DEFAULT 0,
    total_spend           NUMERIC(12,2) DEFAULT 0,
    avg_spend             NUMERIC(10,2),
    min_purchase          NUMERIC(10,2),
    max_purchase          NUMERIC(10,2),
    preferred_browser     VARCHAR(50),
    preferred_os          VARCHAR(50),
    preferred_cc_provider VARCHAR(100),
    preferred_language    VARCHAR(10),
    preferred_time_of_day VARCHAR(2),
    has_expired_cc        BOOLEAN      DEFAULT FALSE,
    unique_cards_used     INTEGER      DEFAULT 1,
    fraud_risk_score      NUMERIC(5,2) DEFAULT 0,
    fraud_risk_category   VARCHAR(20),
    engagement_level      VARCHAR(20),
    effective_date        TIMESTAMP    NOT NULL DEFAULT NOW(),
    expiry_date           TIMESTAMP,
    is_current            BOOLEAN      NOT NULL DEFAULT TRUE,
    version               INTEGER      NOT NULL DEFAULT 1,
    change_reason         VARCHAR(255),
    first_seen_at         TIMESTAMP,
    last_seen_at          TIMESTAMP,
    dw_created_at         TIMESTAMP    DEFAULT NOW(),
    dw_updated_at         TIMESTAMP    DEFAULT NOW()
);
CREATE INDEX idx_dc_customer_id ON dw.dim_customer_scd2(customer_id);
CREATE INDEX idx_dc_is_current  ON dw.dim_customer_scd2(is_current) WHERE is_current = TRUE;

DROP TABLE IF EXISTS dw.fact_transactions CASCADE;
CREATE TABLE dw.fact_transactions (
    transaction_id          VARCHAR(50)  UNIQUE NOT NULL,
    customer_id             VARCHAR(255),
    credit_card_number      VARCHAR(20),
    date_key                INTEGER      REFERENCES dw.dim_date(date_key),
    browser_name            VARCHAR(50),
    os_type                 VARCHAR(50),
    purchase_price          NUMERIC(10,2) NOT NULL,
    am_or_pm                VARCHAR(2),
    language                VARCHAR(10),
    flag_high_value         BOOLEAN      DEFAULT FALSE,
    flag_expired_cc         BOOLEAN      DEFAULT FALSE,
    flag_abnormal_cvv       BOOLEAN      DEFAULT FALSE,
    flag_high_risk_customer BOOLEAN      DEFAULT FALSE,
    total_fraud_flags       INTEGER      DEFAULT 0,
    is_fraud_flagged        BOOLEAN      DEFAULT FALSE,
    source                  VARCHAR(20)  DEFAULT 'batch',
    batch_id                VARCHAR(50),
    ingested_at             TIMESTAMP,
    dw_loaded_at            TIMESTAMP    DEFAULT NOW()
);
CREATE INDEX idx_ft_customer_id ON dw.fact_transactions(customer_id);
CREATE INDEX idx_ft_date_key    ON dw.fact_transactions(date_key);
CREATE INDEX idx_ft_is_fraud    ON dw.fact_transactions(is_fraud_flagged) WHERE is_fraud_flagged = TRUE;

-- Verification
SELECT schemaname AS schema, tablename, 
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema=t.schemaname AND table_name=t.tablename) AS cols
FROM pg_tables t
WHERE schemaname IN ('raw','staging','marts','dw','quality','monitoring')
ORDER BY schemaname, tablename;
