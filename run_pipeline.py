"""End-to-end pipeline runner.

Topology:
    Per-source steps (ingest + bronze) run once per upstream feed.
    Shared steps (silver, matching, gold) run once after all sources are loaded.

NOTE on production deployment:
    In a production Dagster (or Airflow) deployment, the per-source block below
    would be a separate job per source — e.g. `eudragmdp_ingest`,
    `fda_ingest`, `mhra_ingest` — each parameterised with `--source` and
    `--file`. The shared block (silver, matching, gold) would be a downstream
    job that triggers automatically once all per-source jobs finish for a
    given run window. That gives backfills, retries and per-source SLAs for
    free, and prevents cross-source contention on silver/gold.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from ingest import ingest
from matching import run_matching

PROJECT_ROOT = Path(__file__).resolve().parent
DBT_DIR = PROJECT_ROOT / "dbt_project"
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "qualifyze.db"


def _dbt_env() -> dict:
    env = os.environ.copy()
    env["QUALIFYZE_DB_PATH"] = str(DB_PATH)
    env["QUALIFYZE_SCHEMA_DIR"] = str(DATA_DIR)
    return env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", required=True,
        help="Logical source name, e.g. 'eudragmdp'.",
    )
    parser.add_argument(
        "--file", required=True,
        help="Path to the export file for this source.",
    )
    args = parser.parse_args()

    batch_id = "run_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"Starting pipeline run: {batch_id} (source={args.source})")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    env = _dbt_env()

    # ------------------------------------------------------------------
    # Per-source steps
    # ------------------------------------------------------------------
    ingest(batch_id=batch_id, source=args.source, file_path=args.file)

    print("[dbt] dbt seed")
    subprocess.run(
        ["dbt", "seed",
         "--profiles-dir", str(DBT_DIR),
         "--project-dir", str(DBT_DIR)],
        check=True, env=env,
    )

    print("[dbt] dbt run --select bronze")
    subprocess.run(
        ["dbt", "run",
         "--select", "bronze",
         "--vars", f'{{"batch_id": "{batch_id}", "source": "{args.source}"}}',
         "--profiles-dir", str(DBT_DIR),
         "--project-dir", str(DBT_DIR)],
        check=True, env=env,
    )

    # ------------------------------------------------------------------
    # Shared steps - silver, matching, then gold.
    # In production these would live in a downstream Dagster job triggered
    # after all per-source bronze jobs have completed for the run window.
    # ------------------------------------------------------------------
    print("[dbt] dbt run --select silver gold_master_site")
    # gold_master_site is included here because matching.py reads it.
    subprocess.run(
        ["dbt", "run",
         "--select", "silver", "gold_master_site",
         "--vars", f'{{"batch_id": "{batch_id}", "source": "{args.source}"}}',
         "--profiles-dir", str(DBT_DIR),
         "--project-dir", str(DBT_DIR)],
        check=True, env=env,
    )

    run_matching()

    print("[dbt] dbt run --select gold (post-matching)")
    subprocess.run(
        ["dbt", "run",
         "--select", "gold",
         "--vars", f'{{"batch_id": "{batch_id}", "source": "{args.source}"}}',
         "--profiles-dir", str(DBT_DIR),
         "--project-dir", str(DBT_DIR)],
        check=True, env=env,
    )

    print("[dbt] dbt test (informational)")
    subprocess.run(
        ["dbt", "test",
         "--profiles-dir", str(DBT_DIR),
         "--project-dir", str(DBT_DIR)],
        check=False, env=env,
    )

    print(f"Pipeline complete. batch_id={batch_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
