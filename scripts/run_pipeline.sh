#!/bin/bash
# run_pipeline.sh — One-command manual pipeline run (batch only)
set -e
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
ok()   { echo -e "${GREEN}✅ $1${NC}"; }
warn() { echo -e "${YELLOW}⚠️  $1${NC}"; }

log "=========================================="
log "  E-Commerce Batch Pipeline v2 — Manual Run"
log "=========================================="

log "Step 1/4: Batch Ingest CSV → PostgreSQL"
python ingestion/batch_ingest.py --file data/Ecommerce_Purchases.csv --truncate \
  && ok "Batch ingest complete" || { echo -e "${RED}❌ Ingest failed${NC}"; exit 1; }

log "Step 2/4: Data Quality Checks (30 rules)"
python scripts/data_quality_runner.py --dag-id manual_run \
  && ok "DQ checks passed" || warn "Some DQ checks failed (check logs above)"

log "Step 3/4: dbt Transformations"
cd dbt
dbt run --profiles-dir . --target dev && ok "dbt run complete" || { echo -e "${RED}❌ dbt failed${NC}"; exit 1; }
dbt test --profiles-dir . --target dev && ok "dbt test passed" || warn "Some dbt tests failed"
cd ..

log "Step 4/4: Summary"
python3 - << 'PYEOF'
import os, psycopg2
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST','localhost'),
    port=int(os.getenv('POSTGRES_PORT',5433)),
    dbname='ecommerce_db', user='postgres', password='postgres'
)
cur = conn.cursor()
tables = [
    ('raw.ecommerce_purchases',     'Raw data'),
    ('staging.stg_purchases',       'Staging'),
    ('staging.int_customer_stats',  'Customer stats'),
    ('marts.mart_sales_summary',    'Sales mart'),
    ('marts.mart_customer_360',     'Customer 360'),
    ('marts.mart_fraud_signals',    'Fraud signals'),
    ('dw.fact_transactions',        'Fact table'),
]
print("\n  Table                        | Rows")
print("  -----------------------------|-------")
for t, label in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        c = cur.fetchone()[0]
        print(f"  {label:<30}| {c:,}")
    except Exception as e:
        print(f"  {label:<30}| ERROR")

# DQ Summary
cur.execute("SELECT health_score, overall_status, total_rules, passed_rules, failed_rules FROM quality.dq_run_summary ORDER BY started_at DESC LIMIT 1")
row = cur.fetchone()
if row:
    print(f"\n  DQ Health Score: {row[0]:.1f}%  |  Status: {row[1]}  |  {row[3]}/{row[2]} rules passed")
conn.close()
PYEOF

log "=========================================="
ok "Pipeline complete! Open Metabase: http://localhost:3000"
log "=========================================="
