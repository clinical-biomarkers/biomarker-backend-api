import pymongo
from pymongo.collection import Collection
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


def print_indexes(collection: Collection):
    """Format prints the indexes for the target collection."""
    try:
        indexes = collection.index_information()
        for name, index in indexes.items():
            print(f"Index name: {name}")
            print(f"Index details: {index}")
            print()
    except Exception as e:
        print(f"Failed to retrieve indexes: {e}")
        sys.exit(1)


def main():

    parser = argparse.ArgumentParser(prog="peak_collection.py")
    parser.add_argument("server", help="tst/prd")
    parser.add_argument(
        "-b",
        "--biomarker",
        action="store_true",
        help="Store true argument for the biomarker collection.",
    )
    parser.add_argument(
        "-m",
        "--canonical_map",
        action="store_true",
        help="Store true argument for the canonical level id map collection.",
    )
    parser.add_argument(
        "-s",
        "--second_map",
        action="store_true",
        help="Store true argument for the second level collection.",
    )
    parser.add_argument(
        "-e",
        "--error",
        action="store_true",
        help="Store true argument for the error collection.",
    )
    parser.add_argument(
        "-c",
        "--cache",
        action="store_true",
        help="Store true argument for the cache collection.",
    )
    options = parser.parse_args()
    server = options.server.lower().strip()
    if server not in {"tst", "prd"}:
        print("Invalid server.")
        sys.exit(1)
    host_w_port = f"{host}{tst_port}" if server == "tst" else f"{host}{prd_port}"
    option_list = [
        options.biomarker,
        options.canonical_map,
        options.second_map,
        options.error,
        options.cache,
    ]
    if not any(option_list):
        print("Need to specify one collection.")
        parser.print_help()
        sys.exit(0)
    true_list = [x for x in option_list if x]
    if len(true_list) > 1:
        print("Too many collections passed, can only use one at a time.")
        parser.print_help()
        sys.exit(0)

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
    except Exception as e:
        print(e)
        sys.exit(1)

    target_collection_idx = option_list.index(True)
    target_collection = collection_list[target_collection_idx]
    collection = dbh[target_collection]
    print_indexes(collection)


if __name__ == "__main__":
    main()
