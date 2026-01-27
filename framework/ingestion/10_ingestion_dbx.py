# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 10 - Ingestion (Bronze/Landing)
# MAGIC Este notebook realiza una ingesta mínima:
# MAGIC - Lee data/source/customers.csv
# MAGIC - Escribe en data/landing/customers_<timestamp>.csv
# MAGIC - Loggea issues básicos (sin bloquear aún)

# COMMAND ----------

import os

import shutil
from datetime import datetime
from pathlib import Path

# COMMAND ----------

# Parametrización simple
ENV = os.getenv("ENV", "dev")  # para Actions / futuro Databricks Jobs
SOURCE_FILE = Path("data/source/customers.csv")
LANDING_DIR = Path("data/landing")
LOGS_DIR = Path("logs")

# COMMAND ----------



# COMMAND ----------

def run_ingestion():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    # Escribir a landing con timestamp
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    target_file = LANDING_DIR / f"customers_{ts}.csv"

    shutil.copy2(SOURCE_FILE, target_file)

    # Log
    log_msg = (
        f"[OK] Ingestion completed | ENV={ENV}\n"
        f"Source: {SOURCE_FILE}\n"
        f"Target: {target_file}\n"
        f"Timestamp: {ts}\n"
        "------------------------------------\n"
    )

    print(log_msg)
    with open(LOGS_DIR / "ingestion.log", "a", encoding="utf-8") as lf:
        lf.write(log_msg)

# COMMAND ----------

if __name__ == "__main__":
    run_ingestion()
