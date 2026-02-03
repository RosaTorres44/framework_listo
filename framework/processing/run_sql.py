import os
from databricks import sql

HOST = os.environ["DATABRICKS_HOST"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
SQL_FILE = os.environ.get("SQL_FILE")

def main():
    if not SQL_FILE:
        raise ValueError("Missing SQL_FILE env var")

    with open(SQL_FILE, "r", encoding="utf-8") as f:
        script = f.read()

    # separa por ; (suficiente para este taller)
    statements = [s.strip() for s in script.split(";") if s.strip()]

    with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN) as conn:
        with conn.cursor() as cur:
            for st in statements:
                cur.execute(st)

    print(f"✅ Executed SQL file: {SQL_FILE} ({len(statements)} statements)")

if __name__ == "__main__":
    main()
