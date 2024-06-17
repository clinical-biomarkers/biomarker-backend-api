import logging
import os
import sys
import re
import json
import pymongo
from pymongo import MongoClient, errors
from pymongo.database import Database
import subprocess
from typing import Optional, Dict, List, Union, Tuple


def load_json(filepath: str) -> Union[List, Dict]:
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
    with open(filepath, "r") as f:
        json_obj = json.load(f)
    return json_obj


def get_config_details(
    server: str, config_fp: str = "config.json"
) -> Tuple[str, str, str, str, str, str, str, str, str, str]:
    """Gets the config file details.

    Parameters
    ----------
    server : str
        The server to grab data for.
    config_fp : str (default = "config.json")
        The filepath to the config JSON.

    Returns
    -------
    tuple : (str, str, str, str, str, str, str, str, str, str)
        The mongo port, host string, database name, username, password, biomarker collection,
        cache collection, log collection, error collection, and search collection.
    """
    config_obj = load_json(config_fp)
    if not isinstance(config_obj, dict):
        print(
            f"Error reading config JSON, expected type dict and got {type(config_obj)}."
        )
        sys.exit(1)
    server = server.lower().strip()
    if server not in {"tst", "prd"}:
        print(f"Unsupported server argument, expexted `tst` or `prd`, got {server}.")

    mongo_port = config_obj["dbinfo"]["port"][server]
    host = f"mongodb://127.0.0.1:{mongo_port}"
    db_name = config_obj["dbinfo"]["dbname"]
    db_user = config_obj["dbinfo"][db_name]["user"]
    db_pass = config_obj["dbinfo"][db_name]["password"]
    biomarker_collection = config_obj["dbinfo"][db_name]["collection"]
    cache_collection = config_obj["dbinfo"][db_name]["cache_collection"]
    log_collection = config_obj["dbinfo"][db_name]["req_log_collection"]
    error_collection = config_obj["dbinfo"][db_name]["error_log_collection"]
    search_collection = config_obj["dbinfo"][db_name]["search_collection"]

    return (
        mongo_port,
        host,
        db_name,
        db_user,
        db_pass,
        biomarker_collection,
        cache_collection,
        log_collection,
        error_collection,
        search_collection,
    )


def write_json(filepath: str, data: Union[List, Dict]):
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
    db_name: Optional[str] = None,
    authMechanism: str = "SCRAM-SHA-1",
    serverSelectionTimeoutMS: int = 10000,
) -> Union[Database, None]:
    """Gets a MongoDB handle.

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
    Database or None
        The MongoDB database handle or None on error.
    """
    if db_name is None:
        db_name = authSource
    try:
        client: MongoClient = MongoClient(
            host,
            username=username,
            password=password,
            authSource=authSource,
            authMechanism=authMechanism,
            serverSelectionTimeoutMS=serverSelectionTimeoutMS,
        )
        # test the connection
        client.server_info()
    except errors.ServerSelectionTimeoutError as e:
        print(e)
        return None
    except Exception as e:
        print(e)
        return None

    return client[db_name]


def setup_logging(log_path: str):
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


def get_user_confirmation() -> bool:
    """Prompts the user for a confirmation or denial.

    Returns
    ----------
    bool
        Whether the user confirmed or denied.
    """
    while True:
        user_input = input("Continue? (y/n)").strip().lower()
        if user_input == "y":
            return True
        elif user_input == "n":
            return False
        else:
            print("Please enter 'y' for yes or 'n' for no.")
