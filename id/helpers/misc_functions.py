""" Miscellaneous functions and MongoDB functions used throughout the ID assignment process. 

Public functions:
    load_json: Loads data from a JSON file.
    write_json: Writes data to a JSON file.
    get_mongo_handle: Sets up a MongoDB connection and returns a database handle.
    setup_index: Sets up a unique index on a specified field. 
    setup_logging: Sets up a logger.
    validate_filepath: Validates a filepath. 
    copy_file: Copies files from a source to destination.
    clean_value: Removes all non-alphanumeric chracters and lowercases a string.
    get_user_confirmation: Gets user confirmation or deial for an operation.
"""

import logging
import sys
import os
import re
import json
from urllib.parse import quote_plus
import pymongo
import pymongo.errors
from pymongo.database import Database
import subprocess
from typing import Union
from . import id_backend as id_backend


def load_json(filepath: str) -> Union[dict, list]:
    """Loads a JSON file.

    Parameters
    ----------
    filepath: str
        The path to the JSON file.

    Returns
    -------
    dict or list
        The JSON object.
    """
    try:
        with open(filepath, "r") as f:
            json_obj = json.load(f)
    except FileNotFoundError as e:
        print(f"FileNotFoundError in load_json for filepath: `{filepath}`.\n{e}")
        sys.exit(1)
    return json_obj


def write_json(filepath: str, data: Union[list, dict]) -> None:
    """Writes a JSON file.

    Parameters
    ----------
    filepath: str
        The path to the JSON file.
    data: dict or list
        The data to write to the JSON file.
    """
    with open(filepath, "w") as f:
        json.dump(data, f, indent=4)


def get_mongo_handle(
    host: str,
    authSource: str,
    username: str,
    password: str,
    db_name: str = "",
    authMechanism: str = "SCRAM-SHA-1",
    serverSelectionTimeoutMS: int = 10000,
) -> Database:
    """Gets a MongoDB handle. Will exit with status code 1 on error.

    Parameters
    ----------
    host: str
        The MongoDB host (including port).
    authSource: str
        The MongoDB authentication source.
    username: str
        The MongoDB username.
    password: str
        The MongoDB password.
    db_name: str (default = authSource)
        The MongoDB database name.
    authMechanism: str (default = 'SCRAM-SHA-1')
        The MongoDB authentication mechanism.
    serverSelectionTimeoutMS: int (default = 10000)
        The MongoDB server selection timeout in milliseconds.

    Returns
    -------
    Database
        The database handle.
    """
    db_name = db_name or authSource
    try:
        client: pymongo.MongoClient = pymongo.MongoClient(
            host,
            username=username,
            password=password,
            authSource=authSource,
            authMechanism=authMechanism,
            serverSelectionTimeoutMS=serverSelectionTimeoutMS,
        )
        # test the connection
        client.server_info()
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logging.error(f"ServerSelectionTimeoutError retrieving database handle.\n{e}")
        print(f"ServerSelectionTimeoutError retrieving database handle.\n{e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error retrieving database handle.\n{e}")
        print(f"Unexpected error retrieving database handle.\n{e}")
        sys.exit(1)

    return client[db_name]


def create_connection_string(
    host: str,
    username: str,
    password: str,
    authSource: str,
    db_name: str = "",
    authMechanism: str = "SCRAM-SHA-1",
) -> str:
    """Manually creates a connection string.

    Note: Depending on future scaling might have to introduce SRV flag.

    Parameters
    ----------
    host: str
        The MongoDB host (including port).
    authSource: str
        The MongoDB authentication source.
    username: str
        The MongoDB username.
    password: str
        The MongoDB password.
    db_name: str (default = authSource)
        The MongoDB database name.
    authMechanism: str (default = 'SCRAM-SHA-1')
        The MongoDB authentication mechanism.

    Returns
    -------
    str
        The connection string.
    """
    db_name = db_name or authSource
    username = quote_plus(username)
    password = quote_plus(password)
    uri = f"mongodb://{username}:{password}@{host}/{db_name}?authSource={authSource}&authMechanism={authMechanism}"
    return uri


def setup_index(
    dbh, index_col: str, collection_name: str, index_name: str = ""
) -> None:
    """Sets up an index on the specified index_name in the specified collection.

    Parameters
    ----------
    dbh: pymongo.MongoClient
        The database handle.
    index_col: str
        The field to index.
    collection_name: str
        The name of the collection to create the index in.
    index_name: str (default = f'{index_col}_1')
        The name of the index to create.
    """
    if not index_name:
        index_name = f"{index_col}_1"
    if index_name not in dbh[collection_name].index_information():
        dbh[collection_name].create_index(
            [(index_col, pymongo.ASCENDING)], name=index_name, unique=True
        )
        logging.info(
            f"Created index {index_name} on {index_col} in {collection_name} collection."
        )
    else:
        logging.info(
            f"Index {index_name} on {index_col} in {collection_name} collection already exists."
        )


def setup_logging(log_path: str) -> None:
    """Sets up logging.

    Parameters
    ----------
    log_path: str
        The path to the log file.
    """
    logging.basicConfig(
        filename=log_path,
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def validate_filepath(filepath: str, mode: str) -> bool:
    """Validates the filepaths for the user inputted source path and
    the destination path.

    Parameters
    ----------
    filepath: str
        Filepath to the source data dictionary file or the output path.
    mode: str
        Whether checking the output directory path or the input file. ('input' or 'output')

    Returns
    -------
    bool
        True if the filepath is valid, False otherwise.
    """

    if mode == "input":
        if not os.path.isfile(filepath):
            print(f"Error: The (input) file {filepath} does not exist.")
            return False
    elif mode == "output":
        if not os.path.isdir(filepath):
            print(f"Error: The (output) directory {filepath} does not exist.")
            return False
    else:
        print(f"Error: Invalid mode {mode}.")
        return False
    return True


def copy_file(src: str, dest: str) -> bool:
    """Copies a file from src to dest.

    Parameters
    ----------
    src: str
        The source filepath.
    dest: str
        The destination filepath.

    Returns
    -------
    bool
        True if the file was copied successfully, False otherwise.
    """
    if not validate_filepath(src, "input") or not validate_filepath(dest, "output"):
        return False
    dest_file = os.path.join(dest, os.path.basename(src))
    if os.path.exists(dest_file):
        print(f"Error: File {dest_file} already exists.")
        return False
    try:
        subprocess.run(["cp", src, dest], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(e)
        return False


def clean_value(value: str) -> str:
    """Cleans the passed value using regex. Removes all non-alphanumeric
    characters and makes the value lowercase.

    Parameters
    ----------
    value: str
        The value to clean.

    Returns
    -------
    str
        The cleaned value.
    """
    value = re.sub(r"[^a-zA-Z0-9]", "", value).lower()
    return value


def load_map_confirmation(load_map: Union[dict, None], total_files: list) -> tuple:
    """Gets the user confirmation for the load map configuration.

    Parameters
    ----------
    load_map: dict or None
        The load map configuration that determines where each file should be
        loaded or None. If None, assumes everything should be loaded into
        reviewed collection.
    total_files: list
        All the file paths for the files in the current data release directory.

    Returns
    -------
    tuple (list, list)
        The list of reviewed files and list of unreviewed files.
    """

    def __filename_check(config_filenames: list, cleaned_filenames: list) -> list:
        """Loops through a set of files and formats the confirmation
        string and file lists.

        Parameters
        ----------
        config_filenames: list
            List of files from the specified key in the load map.
        cleaned_filenames: list
            Total list of filenames from the current version directory.

        Returns
        -------
        list
            The base name file list for the specified load map key.
        """
        file_list = []
        error_string = ""
        for file in config_filenames:
            if file not in cleaned_filenames:
                error_string += f"Invalid file `{file}` detected.\n"
            else:
                file_list.append(file)
        if error_string:
            print(error_string)
            print("Exiting...")
            sys.exit(1)
        return file_list

    # isolate file names from file paths
    cleaned_filenames = [os.path.basename(x) for x in total_files]
    cleaned_filenames.remove("load_map.json")
    unreviewed_files = __filename_check(
        load_map.get("unreviewed", []) if load_map is not None else [],
        cleaned_filenames,
    )
    reviewed_files = __filename_check(
        load_map.get("reviewed", []) if load_map is not None else cleaned_filenames,
        cleaned_filenames,
    )

    if len(unreviewed_files) == 0:
        unreviewed_files = [
            file for file in cleaned_filenames if file not in reviewed_files
        ]
    if len(reviewed_files) == 0:
        reviewed_files = [
            file for file in cleaned_filenames if file not in unreviewed_files
        ]

    if len(unreviewed_files) + len(reviewed_files) != len(cleaned_filenames):
        print(
            f"Error: mismatch in load map configuration versus total files in the\
            current version directory. Load map has `{len(reviewed_files)}`\
            reviewed files and `{len(unreviewed_files)}` unreviewed files.\
            There are `{len(total_files)}` in the current version directory. Error\
            could also be caused by file mispellings in the load map."
        )
        sys.exit(1)

    print(
        "The following files are marked to be loaded into the unreviewed collection:\n\t"
        + "\n\t".join(unreviewed_files)
    )
    print(
        "The following files are marked to be loaded into the reviewed collection:\n\t"
        + "\n\t".join(reviewed_files)
    )
    get_user_confirmation()
    return reviewed_files, unreviewed_files


def get_user_confirmation() -> None:
    """Prompts the user for a confirmation or denial."""
    while True:
        user_input = input("Continue? (y/n)").strip().lower()
        if user_input == "y":
            return
        elif user_input == "n":
            sys.exit(0)
        else:
            print("Please enter 'y' for yes or 'n' for no.")

def preprocess_checks(data: list) -> bool:
    """Performs preprocessing checks on the data by ensuring ID format
    is valid and collision key is present (essentially chekcing that the
    ID assign process was completed).

    Parameters
    ----------
    data: dict or list
        The data to check.

    Returns
    -------
    bool
        True if all checks pass, False otherwise.
    """
    for document in data:
        canonical_validation = id_backend.validate_id_format(
            document["biomarker_canonical_id"], 0
        )
        second_level_validation = id_backend.validate_id_format(
            document["biomarker_id"], 1
        )
        collisision_key_check = "collision" in document
        preprocess_conditions = (
            canonical_validation and second_level_validation and collisision_key_check
        )
        if not preprocess_conditions:
            return False
    return True
