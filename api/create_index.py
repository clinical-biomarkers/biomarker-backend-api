"""Creates a text index on the biomarker collection.
"""

import pymongo
import sys
import json
import argparse


def main():

    parser = argparse.ArgumentParser(
        prog="create_text_index.py",
    )
    parser.add_argument("-s", "--server", help="tst/prd")
    options = parser.parse_args()
    if not options.server:
        parser.print_help()
        sys.exit(0)
    server = options.server
    if server.lower() not in {"tst", "prd"}:
        print('Invalid server name. Excepcts "tst" or "prd"')
        sys.exit(0)

    config_obj = json.load(open("config.json", "r"))
    mongo_port = config_obj["dbinfo"]["port"][server]
    host = f"mongodb://127.0.0.1:{mongo_port}"
    db_name = config_obj["dbinfo"]["dbname"]
    db_user = config_obj["dbinfo"][db_name]["user"]
    db_pass = config_obj["dbinfo"][db_name]["password"]
    data_collection = config_obj["dbinfo"][db_name]["collection"]

    try:
        client = pymongo.MongoClient(
            host,
            username=db_user,
            password=db_pass,
            authSource=db_name,
            authMechanism="SCRAM-SHA-1",
            serverSelectionTimeoutMS=10000,
        )
        client.server_info()
        dbh = client[db_name]
        result = dbh[data_collection].create_index([("$**", pymongo.TEXT)])
        print(f"result: {result}")
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
