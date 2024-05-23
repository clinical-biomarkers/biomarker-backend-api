""" Handles removing a file from the biomarker collection.
"""

import sys
import json
import pymongo
from pymongo.database import Database
from pymongo.errors import OperationFailure
from helpers import misc_functions as misc_fns
import argparse
import logging

BATCH_SIZE = 1000


def process_data(dbh: Database, data: list, collection_name: str, fp: str) -> bool:
    """Processes the data to remove from the biomarker collection.

    Parameters
    ----------
    dbh : Database
        The database handle.
    data : list
        The data to remove.
    collection_name : str
        The collection to remove from.
    fp : str
        The filepath to the data file.

    Returns
    -------
    bool
        True indicating success, False indicating failure.
    """
    if not misc_fns.preprocess_checks(data):
        logging.error(f"Preprocessing checks failed for file: `{fp}`.")
        print(f"Preprocessing checks failed for file: `{fp}`.")
        return False

    try:
        collection = dbh[collection_name]
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i : i + BATCH_SIZE]
            biomarker_ids_to_remove = [item["biomarker_id"] for item in batch]
            result = collection.delete_many(
                {"biomarker_id": {"$in": biomarker_ids_to_remove}}
            )
            logging.info(
                f"Removed {result.deleted_count} documents in batch {i // BATCH_SIZE}."
            )
            print(
                f"Removed {result.deleted_count} documents in batch {i // BATCH_SIZE}."
            )
    except OperationFailure as ope:
        logging.error(f"Bulk write error: {ope.details}")
        print(f"Bulk write error: {ope.details}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
        return False

    return True


def main():

    parser = argparse.ArgumentParser(prog="remove_data.py")
    parser.add_argument("file_path", help="filepath to the file to be removed.")
    parser.add_argument("-s", "--server", help="tst/prd")
    options = parser.parse_args()
    if len(sys.argv) <= 1:
        sys.argv.append("--help")
    if not options.server:
        parser.print_help()
        sys.exit(1)
    server = options.server
    file_path = options.file_path
    if server.lower not in {"tst", "prd"}:
        print("Invalid server name.")
        sys.exit(1)
    if not misc_fns.validate_filepath(filepath=file_path, mode="input"):
        print("Invalid filepath.")
        sys.exit(1)
    if not file_path.endswith(".json"):
        print("Invalid file extension, expects JSON.")
        sys.exit(1)

    config_obj = misc_fns.load_json("../api/config.json")
    if not isinstance(config_obj, dict):
        print(
            f"Error reading config JSON, expexted type `dict`, got {type(config_obj)}."
        )
        sys.exit(1)
    mongo_port = config_obj["dbinfo"]["port"][server]
    host = f"mongodb://127.0.0.1:{mongo_port}"
    db_name = config_obj["dbinfo"]["dbname"]
    db_user = config_obj["dbinfo"][db_name]["user"]
    db_pass = config_obj["dbinfo"][db_name]["password"]
    data_root_path = config_obj["data_path"]
    canonical_id_collection = config_obj["dbinfo"][db_name]["canonical_id_map"]
    second_level_id_collection = config_obj["dbinfo"][db_name]["second_level_id_map"]
    data_collection = config_obj["dbinfo"][db_name]["collection"]
    dbh = misc_fns.get_mongo_handle(host, db_name, db_user, db_pass)

    misc_fns.setup_logging(f"./logs/remove_data{server}.log")
    logging.info(f"Beginning remove data process: {server}. ####################")

    process_data(dbh, json.load(open(file_path, "r")), data_collection, file_path)


if __name__ == "__main__":
    main()
