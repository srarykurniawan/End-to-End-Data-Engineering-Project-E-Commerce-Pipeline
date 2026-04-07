"""
scripts/diagnose_dq.py
======================
Script diagnostik untuk:
1. Cek versi file data_quality_runner.py yang aktif
2. Inspect isi sql_check dari dq_rules langsung
3. Test eksekusi satu rule secara manual

Cara pakai:
  python scripts/diagnose_dq.py
"""

import os, sys
from pathlib import Path
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=_ROOT / ".env", override=False)

import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB", "ecommerce_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )

print("=" * 65)
print("STEP 1 — Cek file data_quality_runner.py yang aktif")
print("=" * 65)
runner_path = _ROOT / "scripts" / "data_quality_runner.py"
if runner_path.exists():
    content = runner_path.read_text(encoding="utf-8")
    if "table_exists" in content:
        print(f"  ✅ File BARU ditemukan di: {runner_path}")
        print(f"     (mengandung fungsi 'table_exists')")
    elif "safe_failure_pct" in content:
        print(f"  ⚠️  File VERSI TENGAH ditemukan di: {runner_path}")
    else:
        print(f"  ❌ File LAMA masih aktif di: {runner_path}")
        print(f"     Ganti dengan file terbaru dari outputs!")
else:
    print(f"  ❌ File tidak ditemukan di: {runner_path}")

print()
print("=" * 65)
print("STEP 2 — Inspect 5 rule pertama dari quality.dq_rules")
print("=" * 65)

try:
    conn = get_conn()
    print(f"  ✅ Koneksi DB berhasil")

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Cek schema dan tabel dq_rules
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name = 'dq_rules'
        """)
        rows = cur.fetchall()
        if not rows:
            print("  ❌ Tabel 'dq_rules' TIDAK DITEMUKAN di database!")
            sys.exit(1)
        for r in rows:
            print(f"  📋 dq_rules ada di schema: {r['table_schema']}")

        # Ambil 5 rule pertama
        cur.execute("""
            SELECT rule_id, rule_name, target_schema, target_table,
                   target_column, sql_check
            FROM quality.dq_rules
            WHERE is_active = TRUE
            ORDER BY rule_id
            LIMIT 5
        """)
        rules = cur.fetchall()
        print(f"\n  Menampilkan {len(rules)} rule pertama:\n")

        for rule in rules:
            print(f"  ─── Rule: {rule['rule_name']} ───")
            print(f"      target : {rule['target_schema']}.{rule['target_table']}")
            print(f"      column : {rule['target_column']}")
            print(f"      sql    : {rule['sql_check'][:120]}...")
            print()

    print("=" * 65)
    print("STEP 3 — Test eksekusi sql_check rule pertama secara langsung")
    print("=" * 65)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT rule_name, target_schema, target_table, sql_check
            FROM quality.dq_rules
            WHERE is_active = TRUE
            ORDER BY rule_id
            LIMIT 1
        """)
        rule = dict(cur.fetchone())

    print(f"\n  Menjalankan rule: {rule['rule_name']}")
    print(f"  SQL: {rule['sql_check']}\n")

    with conn.cursor() as cur:
        try:
            cur.execute(rule["sql_check"])
            row = cur.fetchone()
            print(f"  fetchone() result  : {row}")
            print(f"  type(row)          : {type(row)}")
            if row:
                print(f"  row[0]             : {row[0]}")
                print(f"  type(row[0])       : {type(row[0])}")
            conn.commit()
            print(f"\n  ✅ SQL check berjalan normal")
        except Exception as e:
            conn.rollback()
            print(f"  ❌ SQL check ERROR: {e}")
            print(f"  ❌ type(error)     : {type(e)}")

    print()
    print("=" * 65)
    print("STEP 4 — Cek tabel yang digunakan oleh rules")
    print("=" * 65)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT DISTINCT target_schema, target_table
            FROM quality.dq_rules
            WHERE is_active = TRUE
            ORDER BY target_schema, target_table
        """)
        tables = cur.fetchall()

    print(f"\n  {'Schema.Table':<45} {'Exists?':<10} {'Row Count'}")
    print(f"  {'-'*45} {'-'*10} {'-'*10}")

    with conn.cursor() as cur:
        for t in tables:
            schema = t["target_schema"]
            table  = t["target_table"]
            try:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                """, (schema, table))
                exists = cur.fetchone()[0]

                if exists:
                    cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                    count = cur.fetchone()[0]
                else:
                    count = "—"

                status = "✅ YES" if exists else "❌ NO"
                print(f"  {schema+'.'+table:<45} {status:<10} {count}")
            except Exception as e:
                conn.rollback()
                print(f"  {schema+'.'+table:<45} ❌ ERROR    {e}")

    conn.close()

except Exception as e:
    print(f"  ❌ Gagal konek ke DB: {e}")
    sys.exit(1)

print()
print("=" * 65)
print("Diagnosis selesai. Baca output di atas untuk menentukan root cause.")
print("=" * 65)