import os
from databricks import sql
from datetime import datetime, timezone

HOST = os.environ["DATABRICKS_HOST"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
RUN_ID = os.environ.get("RUN_ID", "manual")

def q(cur, stmt):
    cur.execute(stmt)
    return cur.fetchall()

def main():
    checked_at = datetime.now(timezone.utc).isoformat()

    with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN) as conn:
        with conn.cursor() as cur:
            total = q(cur, "SELECT COUNT(*) FROM demo.silver_customers")[0][0]
            name_null = q(cur, "SELECT COUNT(*) FROM demo.silver_customers WHERE dq_is_name_null = true")[0][0]
            underage = q(cur, "SELECT COUNT(*) FROM demo.silver_customers WHERE dq_is_underage = true")[0][0]

            # Guardar resultados en tabla ops_dq_results
            cur.execute(
                "INSERT INTO demo.ops_dq_results VALUES (?, current_timestamp(), 'silver_customers', 'total_rows', 'INFO', ?, ?)",
                (RUN_ID, float(total), f"checked_at={checked_at}")
            )
            cur.execute(
                "INSERT INTO demo.ops_dq_results VALUES (?, current_timestamp(), 'silver_customers', 'name_null_count', ?, ?, ?)",
                (RUN_ID, "FAIL" if name_null > 0 else "PASS", float(name_null), "name is null/empty")
            )
            cur.execute(
                "INSERT INTO demo.ops_dq_results VALUES (?, current_timestamp(), 'silver_customers', 'underage_count', ?, ?, ?)",
                (RUN_ID, "WARN" if underage > 0 else "PASS", float(underage), "age < 18")
            )

    print(f"✅ Validation completed. total={total}, name_null={name_null}, underage={underage}")

if __name__ == "__main__":
    main()
