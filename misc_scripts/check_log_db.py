"""Peak into the sqlite logs database.
"""

import sqlite3
import sys
import argparse
import pickle
import json


def deserialize_row(row):
    return tuple(
        pickle.loads(item) if isinstance(item, bytes) else item for item in row
    )


def main():
    parser = argparse.ArgumentParser(prog="check_log_db.py")
    parser.add_argument("server", help="tst/prd")
    parser.add_argument("table", help="api_calls/frontend_logs")
    parser.add_argument("limit", type=int, default=5)
    options = parser.parse_args()

    server = options.server.lower().strip()
    table = options.table.lower().strip()
    limit = options.limit
    if server not in {"tst", "prd"}:
        print("Invalid server.")
        sys.exit(1)
    if table not in {"api_calls", "frontend_logs"}:
        print("Invalid table.")
        sys.exit(1)

    conn = sqlite3.connect(f"/data/shared/biomarkerdb/log_db/{server}/api_logs.db")
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {table} LIMIT {limit}")

    rows = cursor.fetchall()
    for idx, row in enumerate(rows):
        header = "-" * 40
        header += f" Row: {idx} "
        header = "-" * 40
        deserialized_row = deserialize_row(row)
        print(json.dumps(deserialized_row, indent=2, default=str))

    conn.close()


if __name__ == "__main__":
    main()
