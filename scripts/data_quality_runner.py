"""
scripts/data_quality_runner.py  — v4 FINAL
==========================================
Jalankan:
  python scripts/data_quality_runner.py
  python scripts/data_quality_runner.py --dag-id my_dag
"""

import argparse, json, logging, os, sys, time, uuid
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=_ROOT / ".env", override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

_MAX_PCT = 999.9999
_VERSION = "v4-FINAL-2026-03-30"

print(f"[DQ RUNNER] Versi: {_VERSION}  |  File: {__file__}", flush=True)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB", "ecommerce_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def _rb(conn):
    try:
        conn.rollback()
    except Exception:
        pass


def table_exists(conn, schema, table):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name=%s)",
                (schema, table),
            )
            return bool(cur.fetchone()[0])
    except Exception as e:
        _rb(conn)
        return False


def count_rows(conn, schema, table):
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            return cur.fetchone()[0] or 0
    except Exception:
        _rb(conn)
        return 0


def safe_pct(failed, total):
    if failed == 0:
        return 0.0
    d = total if total > 0 else failed
    return min(round(failed / d * 100, 4), _MAX_PCT)


def get_samples(conn, rule):
    schema, table = rule["target_schema"], rule["target_table"]
    col, cat = rule["target_column"], rule["rule_category"]
    if not col:
        return []
    if cat == "completeness":
        sql = f'SELECT {col}, email FROM "{schema}"."{table}" WHERE {col} IS NULL OR TRIM({col}::TEXT)=\'\' LIMIT 5'
    elif cat == "validity" and "price" in col:
        sql = f'SELECT email, {col} FROM "{schema}"."{table}" WHERE {col} < 0 LIMIT 5'
    else:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception:
        _rb(conn)
        return []


def run_one_rule(conn, rule, run_id):
    t0 = time.time()
    schema, table = rule["target_schema"], rule["target_table"]

    res = {
        "run_id": run_id, "rule_id": rule["rule_id"],
        "rule_name": rule["rule_name"], "rule_category": rule["rule_category"],
        "target_table": f"{schema}.{table}", "target_column": rule["target_column"],
        "total_rows": 0, "failed_rows": 0, "passed_rows": 0,
        "failure_pct": 0.0, "threshold_pct": float(rule["threshold_pct"]),
        "status": "ERROR", "severity": rule["severity"],
        "error_message": None, "sample_failures": [], "duration_ms": 0,
    }

    if not table_exists(conn, schema, table):
        res["status"] = "SKIP"
        res["error_message"] = f"Tabel '{schema}.{table}' belum ada di database."
        log.warning(f"  SKIP [{rule['rule_name']}] — {schema}.{table} tidak ditemukan")
        res["duration_ms"] = int((time.time() - t0) * 1000)
        return res

    total = count_rows(conn, schema, table)
    res["total_rows"] = total
    res["passed_rows"] = total

    try:
        with conn.cursor() as cur:
            cur.execute(rule["sql_check"])
            row = cur.fetchone()

        if row is None:
            raise ValueError("sql_check tidak mengembalikan baris apapun.")

        raw = row[0]
        if raw is None:
            raise ValueError("sql_check mengembalikan NULL. Gunakan COALESCE(..., 0).")

        failed = int(raw)
        res["failed_rows"] = failed
        res["passed_rows"] = max(0, total - failed)
        res["failure_pct"] = safe_pct(failed, total)

        if failed == 0:
            res["status"] = "PASS"
        elif res["failure_pct"] <= float(rule["threshold_pct"]):
            res["status"] = "WARN"
        else:
            res["status"] = "FAIL"

        if failed > 0:
            res["sample_failures"] = get_samples(conn, rule)

        conn.commit()

    except Exception as e:
        _rb(conn)
        res["status"] = "ERROR"
        res["error_message"] = str(e)[:500]
        log.error(f"  ERROR [{rule['rule_name']}]: {e}")

    res["duration_ms"] = int((time.time() - t0) * 1000)
    return res


def save_result(conn, r):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO quality.dq_results
                    (run_id,rule_id,rule_name,rule_category,target_table,
                     target_column,total_rows,failed_rows,passed_rows,
                     failure_pct,threshold_pct,status,severity,
                     error_message,sample_failures,duration_ms)
                VALUES
                    (%(run_id)s,%(rule_id)s,%(rule_name)s,%(rule_category)s,
                     %(target_table)s,%(target_column)s,%(total_rows)s,
                     %(failed_rows)s,%(passed_rows)s,%(failure_pct)s,
                     %(threshold_pct)s,%(status)s,%(severity)s,
                     %(error_message)s,%(sample_failures)s,%(duration_ms)s)
            """, {**r, "sample_failures": json.dumps(r["sample_failures"])})
        conn.commit()
    except Exception as e:
        _rb(conn)
        log.error(f"  Gagal save '{r['rule_name']}': {e}")


def run_all_checks(dag_id="manual_run"):
    run_id = str(uuid.uuid4())
    started_at = datetime.now()

    log.info("=" * 65)
    log.info("🔍 DATA QUALITY RUNNER STARTED")
    log.info(f"   Run ID  : {run_id[:16]}...")
    log.info(f"   DAG ID  : {dag_id}")
    log.info(f"   Version : {_VERSION}")
    log.info("=" * 65)

    conn = get_conn()
    results = []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM quality.dq_rules WHERE is_active=TRUE
                ORDER BY CASE severity
                    WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3 WHEN 'LOW' THEN 4 END,
                    rule_category, rule_name
            """)
            rules = [dict(r) for r in cur.fetchall()]
        conn.commit()

        log.info(f"📋 Loaded {len(rules)} active DQ rules\n")

        icons = {"PASS":"✅","FAIL":"❌","WARN":"⚠️ ","ERROR":"💥","SKIP":"⏭️ "}

        for i, rule in enumerate(rules, 1):
            r = run_one_rule(conn, rule, run_id)
            results.append(r)
            save_result(conn, r)
            log.info(
                f"  [{i:2d}/{len(rules)}] {icons.get(r['status'],'?')} "
                f"{r['status']:<5} | {r['rule_name']:<45} | "
                f"failed={r['failed_rows']:,} ({r['failure_pct']:.2f}%)"
            )

        # Summary
        total    = len(results)
        passed   = sum(1 for r in results if r["status"] == "PASS")
        failed   = sum(1 for r in results if r["status"] == "FAIL")
        warned   = sum(1 for r in results if r["status"] == "WARN")
        skipped  = sum(1 for r in results if r["status"] == "SKIP")
        errored  = sum(1 for r in results if r["status"] == "ERROR")
        critical = sum(1 for r in results if r["status"]=="FAIL" and r["severity"]=="CRITICAL")
        evaluable = total - skipped - errored
        score    = round(passed / max(evaluable, 1) * 100, 2)
        overall  = "PASS" if failed == 0 else ("WARN" if critical == 0 else "FAIL")
        completed_at = datetime.now()
        dur = (completed_at - started_at).total_seconds()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO quality.dq_run_summary
                        (run_id,dag_id,total_rules,passed_rules,failed_rules,
                         warned_rules,critical_failures,health_score,
                         overall_status,started_at,completed_at,duration_secs)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (run_id) DO UPDATE SET
                        completed_at=EXCLUDED.completed_at,
                        duration_secs=EXCLUDED.duration_secs
                """, (run_id,dag_id,total,passed,failed,warned,
                      critical,score,overall,started_at,completed_at,dur))
            conn.commit()
        except Exception as e:
            _rb(conn)
            log.error(f"Gagal save summary: {e}")

    finally:
        conn.close()

    log.info("\n" + "=" * 65)
    log.info(f"📊 DQ SUMMARY  |  Health Score: {score:.1f}%")
    log.info(f"   Total: {total:3d}  |  PASS: {passed}  |  FAIL: {failed}  |  WARN: {warned}  |  ERROR: {errored}  |  SKIP: {skipped}")
    log.info(f"   Critical Failures: {critical}  |  Overall: {overall}")
    log.info(f"   Duration: {dur:.2f}s")
    log.info("=" * 65)

    bad   = [r for r in results if r["status"] in ("FAIL","ERROR")]
    skips = [r for r in results if r["status"] == "SKIP"]

    if bad:
        log.warning(f"\n⚠️  RULES NEEDING ATTENTION ({len(bad)}):")
        for r in bad:
            if r["status"] == "ERROR":
                log.error(f"   [ERROR/{r['severity']}] {r['rule_name']} — {r['error_message']}")
            else:
                log.error(f"   [FAIL/{r['severity']}]  {r['rule_name']} — {r['failed_rows']:,} violations ({r['failure_pct']:.2f}%)")

    if skips:
        log.info(f"\n⏭️  SKIPPED — tabel belum ada ({len(skips)}):")
        for r in skips:
            log.info(f"   [{r['severity']}] {r['rule_name']} — {r['target_table']}")

    return {"run_id":run_id,"health_score":score,"overall_status":overall,
            "total":total,"passed":passed,"failed":failed,"warned":warned,
            "errored":errored,"skipped":skipped,"critical_failures":critical}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag-id", default="manual_run")
    args = parser.parse_args()
    out = run_all_checks(dag_id=args.dag_id)
    sys.exit(1 if out["critical_failures"] > 0 else 0)