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

biomarker_collection = "biomarker_collection"
canonical_id_collection = "canonical_id_map_collection"
second_id_collection = "second_id_map_collection"
error_log_collection = "error_log_collection"
cache_collection = "search_cache"
collection_list = [
    biomarker_collection,
    canonical_id_collection,
    second_id_collection,
    error_log_collection,
    cache_collection,
]


def main():

    parser = argparse.ArgumentParser(prog="get_collection_stats.py")
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

        for collection in collection_list:
            collection_handle = dbh[collection]
            document_count = collection_handle.count_documents({})
            stats = dbh.command("collstats", collection)
            print(f"{collection.upper()} Stats:")
            print(f"\tNumber of documents: {document_count}")
            print(f"\tCollection size (in bytes): {stats['size']}")
            print(f"\tAverage document size (in bytes): {stats['avgObjSize']}")
            print(f"\tStorage size (in bytes): {stats['storageSize']}")
            print(f"\tNumber of indexes: {stats['nindexes']}")
            print(f"\tTotal index size (in bytes): {stats['totalIndexSize']}")

    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
