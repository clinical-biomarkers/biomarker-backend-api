import sys
import os
import subprocess
from pymongo import MongoClient
import pymongo
from pymongo.database import Database
from pymongo.collection import Collection
from logging import Logger
from typing import Optional, NoReturn, Literal
from time import time
from urllib.parse import quote_plus
from tutils.config import get_config
from tutils.logging import log_msg, elapsed_time_formatter
from . import ROOT_DIR


def get_database_handle(
    db_name: str,
    port: int,
    username: str,
    password: str,
    host: str = "mongodb://127.0.0.1:",
    auth_source: Optional[str] = None,
    auth_mechanism: str = "SCRAM-SHA-1",
    timeout: int = 1_000,
    logger: Optional[Logger] = None,
) -> Database | NoReturn:
    """Returns a database handle."""
    try:
        auth_source = db_name if auth_source is None else auth_source
        client: MongoClient = MongoClient(
            host=f"{host}{port}",
            username=username,
            password=password,
            authSource=auth_source,
            authMechanism=auth_mechanism,
            serverSelectionTimeoutMS=timeout,
        )
        dbh = client[db_name]
        return dbh
    except Exception as e:
        msg = f"Failed to grab database handle: {str(e)}"
        if logger:
            log_msg(logger=logger, msg=msg, level="error")
        else:
            print(msg)
        sys.exit(1)


def get_standard_db_handle(server: str) -> Database:
    """Gets the standard database handle."""
    config_obj = get_config()
    port = config_obj["dbinfo"]["port"][server]
    db_name = config_obj["dbinfo"]["dbname"]
    db_user = config_obj["dbinfo"][db_name]["user"]
    db_pass = config_obj["dbinfo"][db_name]["password"]
    return get_database_handle(
        db_name=db_name, port=port, username=db_user, password=db_pass
    )


def get_collections() -> dict[str, str]:
    """Gets a list of the collections."""
    config = get_config()
    db_name = config["dbinfo"]["dbname"]
    return config["dbinfo"][db_name]["collections"]


def setup_index(
    collection: Collection,
    index_field: str,
    unique: bool = False,
    order: Literal["ascending", "descending"] = "ascending",
    index_name: Optional[str] = None,
    logger: Optional[Logger] = None,
) -> None:
    """Sets up a regular index on a field.

    Parameters
    ----------
    collection: Collection
        The database collection.
    index_field: str
        The field to index.
    unique: bool, optional
        Whether to make the index unique, defaults to False.
    order: Literal["ascending", "descending"], optional
        Sort order for the index, defaults to "ascending".
    index_name: str, optional
        The name of the index to create, will be assigned a default name if None.
    logger: Logger, optional
        A logger to log status messages to.
    """
    if index_name is None:
        index_name = f"{index_field}_{order}"
    try:
        if index_name not in collection.index_information():
            if order == "ascending":
                collection.create_index(
                    [(index_field, pymongo.ASCENDING)], name=index_name, unique=unique
                )
            elif order == "descending":
                collection.create_index(
                    [(index_field, pymongo.DESCENDING)], name=index_name, unique=unique
                )
            status_message = f"Created `{order}` index `{index_name}` on collection `{collection.name}`."
            if logger is not None:
                log_msg(logger=logger, msg=status_message)
        else:
            if logger is not None:
                log_msg(
                    logger=logger,
                    msg=f"{order.title()} index `{index_name}` on collection `{collection.name}` already exists.",
                )
    except Exception as e:
        msg = (
            "Error while setting up index:\n"
            f"\tCollection: {collection}\n"
            f"\tIndex field: {index_field}\n"
            f"\tUnique: {unique}\n"
            f"\tOrder: {order}\n"
            f"\tIndex name: {index_name}\n"
            f"Error: {e}"
        )
        if logger:
            log_msg(logger=logger, msg=msg, level="error")
            sys.exit(1)


def create_text_index(collection: Collection, logger: Optional[Logger] = None) -> None:
    """Creates a text index on the `all_text` field."""
    index_name = "all_text_text"
    if index_name in collection.index_information():
        if logger:
            log_msg(
                logger=logger,
                msg=f"Text index `{index_name}` already exists on collection `{collection.name}`",
            )
        return
    try:
        collection.create_index([("all_text", "text")])
    except Exception as e:
        if logger:
            log_msg(
                logger=logger,
                msg=f"Failed creating text index on collection: {collection}\n{e}",
                level="error",
            )
        sys.exit(1)
    status_message = (
        f"Created `{index_name}` text index on collection `{collection.name}`."
    )
    if logger is not None:
        log_msg(logger=logger, msg=status_message)


def get_connection_string(
    server: str,
    host: str = "127.0.0.1:",
    auth_source: Optional[str] = None,
    auth_mechanism: str = "SCRAM-SHA-1",
) -> str:
    """Return a connection string."""
    config_obj = get_config()
    db_name = config_obj["dbinfo"]["dbname"]
    port = config_obj["dbinfo"]["port"][server]
    db_user = quote_plus(config_obj["dbinfo"][db_name]["user"])
    db_pass = quote_plus(config_obj["dbinfo"][db_name]["password"])
    auth_source = auth_source if auth_source is not None else db_name
    uri = f"mongodb://{db_user}:{db_pass}@{host}{port}/{db_name}?authSource={auth_source}&authMechanism={auth_mechanism}"
    return uri


def dump_id_collection(
    connection_string: str,
    save_path: str,
    collection: str,
    logger: Optional[Logger] = None,
) -> None:
    """Dumps the ID collections to disk to be used later for replication in the
    production database. Can only be run on the tst server.

    Parameters
    ----------
    connection_string: str
        Connection string for the MongoDB connection.
    save_path: str
        The filepath to the local ID map.
    collection: str
        The collection to dump.
    """
    export_log = os.path.join(ROOT_DIR, "logs", f"mongoexport_{collection}.log")
    mongoexport_cmd = f"mongoexport --uri '{connection_string}' --collection {collection} --out {save_path}"
    command = f"nohup {mongoexport_cmd} > {export_log} 2>&1 &"

    msg = f"Dumping {collection} collection with command `{command}`"
    if logger:
        log_msg(logger=logger, msg=msg)
    else:
        print(msg)

    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        msg = (
            "Failed dumping ID map\n"
            "\tArgs passed:\n"
            f"\t\tConnection string: {connection_string}\n"
            f"\t\tSave path: {save_path}\n"
            f"\t\tCollection: {collection}\n"
            f"Error: {e}"
        )
        if logger:
            log_msg(logger=logger, msg=msg, level="error")
        print(msg)


def load_id_collection(
    connection_string: str,
    load_path: str,
    collection: str,
    logger: Logger,
) -> float | NoReturn:
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
    float
        Elapsed time in seconds.
    """
    import_log = os.path.join(ROOT_DIR, "logs", f"mongoimport_{collection}.log")
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

    msg = f"Loading {collection} collection with command: `" + " ".join(command) + "`"
    log_msg(logger=logger, msg=msg)

    try:
        start_time = time()
        with open(import_log, "w") as f:
            subprocess.run(command, check=True, stdout=f, stderr=subprocess.STDOUT)
        elapsed_time = time() - start_time
        msg = f"Finished loading {collection} collection, took {elapsed_time_formatter(elapsed_time)}"
        log_msg(logger=logger, msg=msg)
        return elapsed_time
    except subprocess.CalledProcessError as e:
        msg = (
            f"Error, check the logs at {import_log}\n"
            "Args passed:\n"
            f"\tConnection string: {connection_string}\n"
            f"\tLoad path: {load_path}\n"
            f"\tCollection: {collection}\n"
            f"Error: {e}"
        )
        log_msg(logger=logger, msg=msg, level="error")
        sys.exit(1)
