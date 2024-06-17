""" Script that handles the data load into the MongoDB instance.
"""

import subprocess
import sys
import json
import os
import glob
import pymongo
from pymongo.database import Database
from pymongo.errors import BulkWriteError
from helpers import misc_functions as misc_fns
import argparse
import logging

BATCH_SIZE = 1000


def process_bulk_operations(
    dbh: Database, db_collection: str, bulk_ops: list, fp: str
) -> bool:
    """Handles the bulk write operation to MongoDB.

    Parameters
    ----------
    dbh : Database
        The database handle.
    db_collection : str
        The collection to push to.
    bulk_ops : list
        The list of MongoDB operations to perform.
    fp : str
        The file path of the file currently being processed.

    Returns
    -------
    bool
        True on success and Fale if an exception was caught.
    """
    try:
        dbh[db_collection].bulk_write(bulk_ops, ordered=False)
        return True
    except BulkWriteError as e:
        error_details = e.details
        if "writeErrors" in error_details:
            for error in error_details["writeErrors"]:
                logging.error(
                    f"\nError in operation {error['op']} (in file {fp}):\n\tError: {error['errmsg']}."
                )
        return False
    except Exception as e:
        logging.error(
            f"\nUnexpected error during bulk ops write:\n\tFile: {fp}\n\tError: {e}."
        )
        return False


def handle_upsert_writes(data: list, dbh: Database, collection: str, fp: str) -> int:
    """Handles the upsert mode overwrites and inserts. Iterates through
    the entries in the data list and if the entry ID exists in the database
    collection the document will be overwritten. If the entry ID doesn't
    exist a new document will be added. The collision keys will be updated as
    appropriate and the source file will be overwritten with the updated collision
    keys.

    Parameters
    ----------
    data : list
        The data to load in upsert mode.
    dbh : Databse
        The database handle.
    collection : str
        The name of the data collection.
    fp : str
        The filepath to the data file.

    Returns
    -------
    int
        0 if completed successfully, 1 if partial success, and 2 if full failure.
    """
    success_count = 0
    updated_data = []
    for idx, document in enumerate(data):
        document.pop("collision")
        try:
            dbh[collection].find_one_and_replace(
                {"biomarker_id": document["biomarker_id"]}, document, upsert=True
            )
            success_count += 1
            document["collision"] = 0
            updated_data.append(document)
        except Exception as e:
            logging.error(
                f"Unexpected error during upsert on file: {fp}\n\tRecord index: {idx}\n\tError: {e}\n\tDocument: {document}"
            )
            print(
                f"Unexpected error during upsert on file: {fp}\n\tRecord index: {idx}\n\tError: {e}\n\tDocument: {document}"
            )
    if success_count == len(data):
        misc_fns.write_json(fp, updated_data)
        logging.info(
            f"Successfully loaded upsert file: `{os.path.basename(fp)}` and overwrote existing file for updated collision keys."
        )
        print(
            f"Successfully loaded upsert file: `{os.path.basename(fp)}` and overwrote existing file for updated collision keys."
        )
        return 0
    elif success_count >= 0:
        logging.warning(
            f"Partial error loading upsert file: `{os.path.basename(fp)}`, not overwriting file."
        )
        print(
            f"Partial error loading upsert file: `{os.path.basename(fp)}`, not overwriting file."
        )
        return 1
    else:
        logging.error(
            f"Full error loading upsert file: `{os.path.basename(fp)}`, not overwriting file."
        )
        print(
            f"Full error loading upsert file: `{os.path.basename(fp)}`, not overwriting file."
        )
        return 2


def process_data(
    data: list,
    dbh: Database,
    db_collection: str,
    collision_collection: str,
    fp: str,
    collision_full: bool,
    upsert_mode: bool,
) -> int:
    """Inserts the data into the database.

    Parameters
    ----------
    data : list
        The data to process.
    dbh : Database
        The database handle.
    db_collection : str
        The name of the collection to insert the data into.
    collision_collection : str
        The name of the collection to insert the unreviewed data into.
    fp : str
        The filepath to the data file.
    collision_full : bool
        Whether to entirely load into the unreviewed collection.
    upsert_mode : bool
        Whether the file was marked for upsert mode.

    Returns
    -------
    int
        0 if completed successfully, 1 if partial success, and 2 if full failure.
    """
    if not misc_fns.preprocess_checks(data):
        logging.error(f"Preprocessing checks failed for file: '{fp}'.")
        print(f"Preprocessing checks failed for file: '{fp}'.")
        return 2

    if upsert_mode:
        upsert_result = handle_upsert_writes(data, dbh, db_collection, fp)
        return upsert_result

    bulk_ops = []
    bulk_write_results = []
    collision_ops = []

    for idx, document in enumerate(data):

        collision_status = document.pop("collision")
        if collision_status == 2:
            logging.info(
                f"Skipping index `{idx}` in file `{os.path.basename(fp)}` for hard collision\
                \n\tbiomarker canonical ID: `{document['biomarker_canonical_id']}`\
                \n\tbiomarker ID: `{document['biomarker_id']}`"
            )
            continue
        elif collision_status == 0:
            if collision_full:
                collision_ops.append(pymongo.InsertOne(document))
            else:
                bulk_ops.append(pymongo.InsertOne(document))
        elif collision_status == 1:
            collision_ops.append(pymongo.InsertOne(document))

        if len(bulk_ops) >= BATCH_SIZE:
            bulk_write_results.append(
                process_bulk_operations(dbh, db_collection, bulk_ops, fp)
            )
            bulk_ops = []
        if len(collision_ops) >= BATCH_SIZE:
            bulk_write_results.append(
                process_bulk_operations(dbh, collision_collection, collision_ops, fp)
            )
            collision_ops = []

    if bulk_ops:
        bulk_write_results.append(
            process_bulk_operations(dbh, db_collection, bulk_ops, fp)
        )
    if collision_ops:
        bulk_write_results.append(
            process_bulk_operations(dbh, collision_collection, collision_ops, fp)
        )

    if all(bulk_write_results):
        return 0
    elif any(bulk_write_results):
        return 1
    else:
        return 2


def load_id_collection(connection_string: str, load_path: str, collection: str) -> bool:
    """Loads the local ID collections into the prod database.

    Parameters
    ----------
    connection_string : str
        Connection string for the MongoDB connection.
    load_path : str
        The filepath to the local ID map.
    collection : str
        The collection to load into.

    Returns
    -------
    bool
        Indication if the collection was loaded successfully.
    """
    command = [
        "mongoimport",
        "--uri",
        connection_string,
        "--collection",
        collection,
        "--file",
        load_path,
        "--mode",
        "upsert",
    ]

    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print("Args passed:")
        print(f"Connection string: {connection_string}")
        print(f"Load path: {load_path}")
        print(f"Collection: {collection}")
        print(e)
        return False
    return True


def main():

    ### handle command line arguments
    parser = argparse.ArgumentParser(
        prog="load_data.py", usage="python load_data.py [options] server"
    )
    parser.add_argument("-s", "--server", help="tst/prd")
    parser.add_argument(
        "-u", "--upsert_file", help="Specify a file to run in upsert mode."
    )
    options = parser.parse_args()
    if not options.server:
        parser.print_help()
        sys.exit(1)
    server = options.server
    if server.lower() not in {"tst", "prd"}:
        print("Invalid server name.")
        sys.exit(1)
    upsert_file = options.upsert_file
    if upsert_file is None:
        upsert_file = ""

    ### get config info
    config_obj = misc_fns.load_json("../api/config.json")
    if not isinstance(config_obj, dict):
        print(
            f"Error reading config JSON, expected type `dict`, got {type(config_obj)}."
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
    unreviewed_collection = config_obj["dbinfo"][db_name]["unreviewed_collection"]
    dbh = misc_fns.get_mongo_handle(host, db_name, db_user, db_pass)

    ### setup logger
    misc_fns.setup_logging(f"./logs/load_data_{server}.log")
    logging.info(f"Loading data for server: {server}. #####################")

    ### load the load map
    try:
        load_map = json.load(
            open(
                f"{data_root_path}/generated/datamodel/new_data/current/load_map.json",
                "r",
            )
        )
        if not isinstance(load_map, dict):
            print(
                f"Error reading load map JSON, expected type `dict`, got {type(load_map)}."
            )
            sys.exit(1)
    except FileNotFoundError:
        load_map = None

    ### begin processing data
    data_release_glob_pattern = (
        f"{data_root_path}/generated/datamodel/new_data/current/*.json"
    )
    total_files = glob.glob(data_release_glob_pattern)
    _, unreviewed_files = misc_fns.load_map_confirmation(load_map, total_files)
    if upsert_file != "":
        if upsert_file in unreviewed_files:
            print(
                f"Configuration error: File {upsert_file} marked for upsert mode but listed in unreviewed files in load map."
            )
            sys.exit(1)
        print(f"File {upsert_file} marked to be loaded in upsert mode.")
        misc_fns.get_user_confirmation()

    # if running on prd server, load the id_collection.json file
    if server == "prd":
        canonical_id_collection_local_path = (
            f"{data_root_path}/generated/datamodel/canonical_id_collection.json"
        )
        second_level_id_collection_local_path = (
            f"{data_root_path}/generated/datamodel/second_level_id_collection.json"
        )
        connection_string = misc_fns.create_connection_string(
            f"127.0.0.1:{mongo_port}", db_user, db_pass, db_name
        )
        if load_id_collection(
            connection_string,
            canonical_id_collection_local_path,
            canonical_id_collection,
        ):
            print("Successfully loaded canonical ID map into prod database.")
        else:
            print(
                "Failed loading canonical ID map into prod database. You will have to update manually."
            )
        if load_id_collection(
            connection_string,
            second_level_id_collection_local_path,
            second_level_id_collection,
        ):
            print("Successfully loaded secondary ID map into prod database.")
        else:
            print(
                "Failed loading secondary ID map into prod database. You will have to update manually."
            )

    # process each data file
    for fp in glob.glob(data_release_glob_pattern):
        if fp == f"{data_root_path}/generated/datamodel/new_data/current/load_map.json":
            continue
        data = misc_fns.load_json(fp)
        if not isinstance(data, list):
            print(
                f"Error reading data file `{fp}`, expected list, got `{type(data)}`. Skipping..."
            )
            continue

        upsert_mode = os.path.basename(fp) == upsert_file

        if os.path.basename(fp) in unreviewed_files:
            collision_full = True
        else:
            collision_full = False

        result = process_data(
            data,
            dbh,
            data_collection,
            unreviewed_collection,
            fp,
            collision_full,
            upsert_mode,
        )
        if not upsert_mode:
            if result == 0:
                if collision_full:
                    output = f"Successfully loaded data into unreviewed collection for file: {fp}."
                else:
                    output = f"Successfully loaded data into reviewed collection for file: {fp}."
            elif result == 1:
                if collision_full:
                    output = f"Partial sucess loading data into unreviewed collection for file: {fp}.\n\tCheck logs."
                else:
                    output = f"Partial sucess loading data into reviewed collection for file: {fp}.\n\tCheck logs."
            else:
                if collision_full:
                    output = f"Failed to load data entirely into unreviewed collection for file: {fp}.\n\tCheck logs."
                else:
                    output = f"Failed to load data entirely into reviewed collection for file: {fp}.\n\tCheck logs."
            logging.error(output)
            print(output)

    logging.info(f"Finished loading data for server: {server}. ---------------------")


if __name__ == "__main__":
    main()
