"""Clears some supplementary collections.
"""

import pymongo
import sys
import json
import argparse
import misc_functions as misc_fns


def main():

    parser = argparse.ArgumentParser(prog="clear_collections.py")
    parser.add_argument("server", help="server to clear ")
    parser.add_argument(
        "-c",
        "--cache",
        action="store_false",
        help="Store false argument for clearing the cache collection.",
    )
    parser.add_argument(
        "-l",
        "--log",
        action="store_false",
        help="Store false argument for clearing the log collection.",
    )
    parser.add_argument(
        "-e",
        "--error",
        action="store_false",
        help="Store false argument for clearing the error log collection.",
    )
    options = parser.parse_args()
    server = options.server.lower().strip()
    if server not in {"tst", "prd"}:
        print("Invalid server.")
        sys.exit(0)

    confimation_message = "Clearing the following collections:"
    if options.cache:
        confimation_message += "\n\tCache collection"
    if options.log:
        confimation_message += "\n\tLog collection"
    if options.error:
        confimation_message += "\n\tError collection"

    print(confimation_message)
    if not misc_fns.get_user_confirmation():
        print("Exiting...")
        sys.exit(0)

    config_obj = json.load(open("config.json", "r"))
    mongo_port = config_obj["dbinfo"]["port"][server]
    host = f"mongodb://127.0.0.1:{mongo_port}"
    db_name = config_obj["dbinfo"]["dbname"]
    db_user = config_obj["dbinfo"][db_name]["user"]
    db_pass = config_obj["dbinfo"][db_name]["password"]

    cache_collection = config_obj["dbinfo"][db_name]["cache_collection"]
    log_collection = config_obj["dbinfo"][db_name]["req_log_collection"]
    error_collection = config_obj["dbinfo"][db_name]["error_log_collection"]

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
    except Exception as e:
        print(e)
        sys.exit(1)

    if options.cache:
        try:
            dbh[cache_collection].delete_many({})
            print("Cache collection cleared.")
        except Exception as e:
            print(f"Error clearing cache collection.\n{e}")

    if options.log:
        try:
            dbh[log_collection].delete_many({})
            print("Log collection cleared.")
        except Exception as e:
            print(f"Error clearing log collection.\n{e}")

    if options.error:
        try:
            dbh[error_collection].delete_many({})
            print("Error collection cleared.")
        except Exception as e:
            print(f"Error clearing error collection.\n{e}")


if __name__ == "__main__":
    main()
