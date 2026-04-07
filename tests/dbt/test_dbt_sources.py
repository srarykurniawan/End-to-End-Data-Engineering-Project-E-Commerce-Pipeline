"""
tests/dbt/test_dbt_sources.py
Integration tests that verify dbt model outputs in PostgreSQL.
Requires: running PostgreSQL with pipeline data already loaded.
Run: pytest tests/dbt/ -v --tb=short
"""

import os
import pytest
import psycopg2

# ── Connection ────────────────────────────────────────────────────────────────
@pytest.fixture(scope='module')
def conn():
    c = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=int(os.environ.get('POSTGRES_PORT', 5433)),
        dbname=os.environ.get('POSTGRES_DB', 'ecommerce_db'),
        user=os.environ.get('POSTGRES_USER', 'postgres'),
        password=os.environ.get('POSTGRES_PASSWORD', 'postgres'),
    )
    yield c
    c.close()


def query_one(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


# ── Raw layer ─────────────────────────────────────────────────────────────────
class TestRawLayer:

    def test_raw_purchases_not_empty(self, conn):
        count = query_one(conn, "SELECT COUNT(*) FROM raw.ecommerce_purchases")
        assert count > 0, "raw.ecommerce_purchases should have rows"

    def test_raw_purchases_no_null_email(self, conn):
        nulls = query_one(conn, "SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE email IS NULL")
        assert nulls == 0

    def test_raw_purchases_no_negative_price(self, conn):
        neg = query_one(conn, "SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE purchase_price < 0")
        assert neg == 0

    def test_raw_purchases_valid_ampm(self, conn):
        invalid = query_one(conn, "SELECT COUNT(*) FROM raw.ecommerce_purchases WHERE am_or_pm NOT IN ('AM','PM')")
        assert invalid == 0


# ── Staging layer ─────────────────────────────────────────────────────────────
class TestStagingLayer:

    def test_stg_purchases_row_count(self, conn):
        raw   = query_one(conn, "SELECT COUNT(*) FROM raw.ecommerce_purchases")
        stg   = query_one(conn, "SELECT COUNT(*) FROM staging.stg_purchases")
        assert stg > 0
        assert stg <= raw, "Staging should have same or fewer rows than raw"

    def test_stg_purchases_unique_purchase_id(self, conn):
        total  = query_one(conn, "SELECT COUNT(*) FROM staging.stg_purchases")
        unique = query_one(conn, "SELECT COUNT(DISTINCT purchase_id) FROM staging.stg_purchases")
        assert total == unique, "purchase_id must be unique"

    def test_stg_purchases_valid_price_tier(self, conn):
        invalid = query_one(conn, """
            SELECT COUNT(*) FROM staging.stg_purchases
            WHERE price_tier NOT IN ('Low','Medium','High','Premium')
        """)
        assert invalid == 0

    def test_int_customer_stats_one_row_per_email(self, conn):
        total  = query_one(conn, "SELECT COUNT(*) FROM staging.int_customer_stats")
        unique = query_one(conn, "SELECT COUNT(DISTINCT email) FROM staging.int_customer_stats")
        assert total == unique


# ── Marts layer ───────────────────────────────────────────────────────────────
class TestMartsLayer:

    def test_mart_sales_summary_ten_providers(self, conn):
        count = query_one(conn, "SELECT COUNT(*) FROM marts.mart_sales_summary")
        assert count == 10, f"Expected 10 CC providers, got {count}"

    def test_mart_sales_revenue_share_sums_100(self, conn):
        total = query_one(conn, "SELECT ROUND(SUM(revenue_share_pct)::numeric, 0) FROM marts.mart_sales_summary")
        assert total == 100, f"Revenue share should sum to 100%, got {total}"

    def test_mart_customer_360_unique_email(self, conn):
        total  = query_one(conn, "SELECT COUNT(*) FROM marts.mart_customer_360")
        unique = query_one(conn, "SELECT COUNT(DISTINCT email) FROM marts.mart_customer_360")
        assert total == unique

    def test_mart_customer_360_valid_segments(self, conn):
        invalid = query_one(conn, """
            SELECT COUNT(*) FROM marts.mart_customer_360
            WHERE customer_segment NOT IN ('VIP','Regular','Occasional','One-time')
        """)
        assert invalid == 0

    def test_mart_customer_360_fraud_score_range(self, conn):
        out_of_range = query_one(conn, """
            SELECT COUNT(*) FROM marts.mart_customer_360
            WHERE fraud_risk_score < 0 OR fraud_risk_score > 100
        """)
        assert out_of_range == 0

    def test_mart_fraud_signals_total_flags_range(self, conn):
        invalid = query_one(conn, """
            SELECT COUNT(*) FROM marts.mart_fraud_signals
            WHERE total_flags < 0 OR total_flags > 4
        """)
        assert invalid == 0

    def test_mart_fraud_referential_integrity(self, conn):
        orphans = query_one(conn, """
            SELECT COUNT(*) FROM marts.mart_fraud_signals f
            LEFT JOIN marts.mart_customer_360 c ON f.email = c.email
            WHERE c.email IS NULL
        """)
        assert orphans == 0, "All fraud signals should match a customer"


# ── DW layer ──────────────────────────────────────────────────────────────────
class TestDWLayer:

    def test_dim_date_populated(self, conn):
        count = query_one(conn, "SELECT COUNT(*) FROM dw.dim_date")
        assert count > 5000, "dim_date should have many years of dates"

    def test_dim_customer_no_duplicate_current(self, conn):
        dups = query_one(conn, """
            SELECT COUNT(*) FROM (
                SELECT customer_id, COUNT(*) AS cnt
                FROM dw.dim_customer
                WHERE is_current = TRUE
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            ) x
        """)
        assert dups == 0, "Each customer should have exactly one current version"

    def test_dim_customer_version_1_exists(self, conn):
        no_v1 = query_one(conn, """
            SELECT COUNT(*) FROM (
                SELECT customer_id
                FROM dw.dim_customer
                GROUP BY customer_id
                HAVING MIN(version) != 1
            ) x
        """)
        assert no_v1 == 0, "All customers should start at version 1"
