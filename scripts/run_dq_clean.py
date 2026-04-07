"""
scripts/run_dq_clean.py
========================
Hapus semua __pycache__ dan .pyc lama, lalu jalankan DQ runner bersih.

Cara pakai:
  python scripts/run_dq_clean.py
  python scripts/run_dq_clean.py --dag-id my_dag
"""

import sys, shutil, subprocess, argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

print("🧹 Membersihkan __pycache__ dan .pyc lama...")
removed = 0

for pycache in ROOT.rglob("__pycache__"):
    shutil.rmtree(pycache, ignore_errors=True)
    print(f"   Hapus: {pycache}")
    removed += 1

for pyc in ROOT.rglob("*.pyc"):
    pyc.unlink(missing_ok=True)
    print(f"   Hapus: {pyc}")
    removed += 1

print(f"   ✅ Selesai — {removed} item dihapus\n")

# Jalankan DQ runner dengan interpreter yang sama
parser = argparse.ArgumentParser()
parser.add_argument("--dag-id", default="manual_run")
args = parser.parse_args()

runner = ROOT / "scripts" / "data_quality_runner.py"
cmd    = [sys.executable, str(runner), "--dag-id", args.dag_id]

print(f"🚀 Menjalankan: {' '.join(cmd)}\n")
result = subprocess.run(cmd)
sys.exit(result.returncode)