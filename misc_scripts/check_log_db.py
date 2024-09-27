"""Peak into the sqlite log database.

usage: parser.py [-h] server table limit

positional arguments:
  server      prd/beta/tst/dev
  table       api_calls/frontend_logs
  limit

options:
  -h, --help  show this help message and exit
"""

import sqlite3
import sys
import pickle
import json
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config


def deserialize_row(row):
    return tuple(
        pickle.loads(item) if isinstance(item, bytes) else item for item in row
    )


def main():
    parser, server_list = standard_parser()
    parser.add_argument("table", help="api_calls/frontend_logs")
    parser.add_argument("limit", type=int, default=5)
    options = parser.parse_args()

    server = parse_server(parser=parser, server=options.server, server_list=server_list)
    table = options.table.lower().strip()
    limit = options.limit
    if table not in {"api_calls", "frontend_logs"}:
        print("Invalid table.")
        sys.exit(1)

    config_obj = get_config()
    data_root_path = config_obj["data_path"]
    sqlite_db_path = os.path.join(data_root_path, "log_db", server, "api_logs.db")

    conn = sqlite3.connect(sqlite_db_path)
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
