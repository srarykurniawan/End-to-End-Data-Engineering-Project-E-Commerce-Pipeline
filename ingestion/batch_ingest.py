"""
ingestion/batch_ingest.py — Batch CSV Ingest to PostgreSQL
==========================================================
Satu-satunya jalur ingestion (tidak ada streaming).
Flow: CSV → clean → validate → PostgreSQL raw.ecommerce_purchases
"""

import os, sys, uuid, logging, argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=_ROOT / ".env", override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5433)),
        dbname=os.getenv("POSTGRES_DB", "ecommerce_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        connect_timeout=10,
    )


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {
        "Address": "address", "Lot": "lot", "AM or PM": "am_or_pm",
        "Browser Info": "browser_info", "Company": "company",
        "Credit Card": "credit_card", "CC Exp Date": "cc_exp_date",
        "CC Security Code": "cc_security_code", "CC Provider": "cc_provider",
        "Email": "email", "Job": "job", "IP Address": "ip_address",
        "Language": "language", "Purchase Price": "purchase_price",
    }
    df = df.rename(columns=col_map)
    str_cols = ["address","lot","browser_info","company","email","job","cc_provider","ip_address","language"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    df["am_or_pm"]       = df["am_or_pm"].str.upper().str.strip()
    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors="coerce")
    df["credit_card"]    = df["credit_card"].astype(str).str.strip()
    df["cc_security_code"] = df["cc_security_code"].astype(str).str.strip()
    df["language"]       = df["language"].str.lower().str.strip()
    invalid_ampm = ~df["am_or_pm"].isin(["AM","PM"])
    if invalid_ampm.sum() > 0:
        logger.warning(f"  ⚠️  {invalid_ampm.sum()} invalid am_or_pm → set NULL")
        df.loc[invalid_ampm, "am_or_pm"] = None
    df = df.where(pd.notnull(df), None)
    df = df.replace("nan", None)
    logger.info(f"  Cleaned shape: {df.shape}")
    return df


def validate_dataframe(df: pd.DataFrame) -> dict:
    issues = {}
    for col in ["email","purchase_price","cc_provider"]:
        n = df[col].isna().sum() if col in df.columns else len(df)
        if n > 0:
            issues[f"null_{col}"] = int(n)
    neg = (df["purchase_price"] < 0).sum() if "purchase_price" in df.columns else 0
    if neg > 0:
        issues["negative_price"] = int(neg)
    if issues:
        logger.warning(f"  Validation issues: {issues}")
    else:
        logger.info("  ✅ All validations passed")
    return issues


def run_batch_ingest(csv_path: str, batch_size: int = 1000, truncate: bool = False) -> dict:
    run_id     = str(uuid.uuid4())
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"🚀 BATCH INGEST STARTED  |  run_id={run_id[:8]}...")
    logger.info(f"   File: {csv_path}  |  Batch size: {batch_size}")
    logger.info("=" * 60)

    if not Path(csv_path).exists():
        raise FileNotFoundError(f"File tidak ditemukan: {csv_path}")

    df = pd.read_csv(csv_path)
    total = len(df)
    logger.info(f"📂 CSV loaded: {total:,} rows")

    df = clean_dataframe(df)
    issues = validate_dataframe(df)

    conn = get_conn()
    written = 0
    failed  = 0

    try:
        with conn.cursor() as cur:
            if truncate:
                logger.warning("  ⚠️  TRUNCATE mode — deleting existing data...")
                cur.execute("TRUNCATE TABLE raw.ecommerce_purchases RESTART IDENTITY")
                conn.commit()

            logger.info(f"📥 Inserting {total:,} rows in batches of {batch_size}...")
            for batch_num, start in enumerate(range(0, len(df), batch_size), 1):
                chunk = df.iloc[start: start + batch_size]
                rows = [
                    (r.address, r.lot, r.am_or_pm, r.browser_info, r.company,
                     r.credit_card, r.cc_exp_date, r.cc_security_code, r.cc_provider,
                     r.email, r.job, r.ip_address, r.language, r.purchase_price,
                     run_id, "csv_batch")
                    for r in chunk.itertuples(index=False)
                ]
                try:
                    execute_values(cur, """
                        INSERT INTO raw.ecommerce_purchases
                            (address, lot, am_or_pm, browser_info, company,
                             credit_card, cc_exp_date, cc_security_code, cc_provider,
                             email, job, ip_address, language, purchase_price,
                             batch_id, source)
                        VALUES %s
                    """, rows, page_size=500)
                    conn.commit()
                    written += len(rows)
                    logger.info(f"  Batch {batch_num:3d} | rows {start:6,}–{start+len(chunk):6,} | total {written:,}")
                except Exception as e:
                    conn.rollback()
                    failed += len(rows)
                    logger.error(f"  ❌ Batch {batch_num} failed: {e}")

        # Log to monitoring
        end_time = datetime.now()
        dur = (end_time - start_time).total_seconds()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO monitoring.pipeline_runs
                    (run_id, dag_id, task_id, status, records_read,
                     records_written, records_failed, start_time, end_time,
                     duration_secs)
                VALUES (%s,'batch_ingest','ingest_csv','success',%s,%s,%s,%s,%s,%s)
            """, (run_id, total, written, failed, start_time, end_time, dur))
        conn.commit()

    except Exception as e:
        logger.error(f"❌ Ingest FAILED: {e}")
        raise
    finally:
        conn.close()

    logger.info("=" * 60)
    logger.info(f"✅ DONE | read={total:,} | written={written:,} | failed={failed}")
    logger.info(f"   Duration: {(datetime.now()-start_time).total_seconds():.2f}s")
    logger.info("=" * 60)
    return {"run_id": run_id, "records_read": total,
            "records_written": written, "records_failed": failed,
            "start_time": start_time}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file",       default="data/Ecommerce_Purchases.csv")
    parser.add_argument("--batch-size", default=1000, type=int)
    parser.add_argument("--truncate",   action="store_true")
    args = parser.parse_args()
    result = run_batch_ingest(args.file, args.batch_size, args.truncate)
    sys.exit(0 if result["records_failed"] == 0 else 1)
