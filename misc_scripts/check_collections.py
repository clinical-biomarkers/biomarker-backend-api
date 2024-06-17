import pymongo
import sys
import argparse

host = "mongodb://127.0.0.1:"
tst_port = "6061"
prd_port = "7071"
db_name = "biomarkerdb_api"
db_user = "biomarkeradmin"
db_pass = "biomarkerpass"
auth_mechanism = "SCRAM-SHA-1"


def main():

    parser = argparse.ArgumentParser(prog="peak_collection.py")
    parser.add_argument("server", help="tst/prd")
    options = parser.parse_args()
    server = options.server.lower().strip()
    if server not in {"tst", "prd"}:
        print("Invalid server.")
        sys.exit(1)
    host_w_port = f"{host}{tst_port}" if server == "tst" else f"{host}{prd_port}"

    try:
        client = pymongo.MongoClient(
            host_w_port,
            username=db_user,
            password=db_pass,
            authSource=db_name,
            authMechanism=auth_mechanism,
            serverSelectionTimeoutMS=1000,
        )
        client.server_info()
        dbh = client[db_name]
        collections = dbh.list_collection_names()
        print("Collections:")
        for collection in collections:
            print(f"- {collection}")
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
