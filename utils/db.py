import sys
from pymongo import MongoClient
from pymongo.database import Database
from typing import Optional
from utils.config import get_config


def get_database_handle(
    db_name: str,
    port: int,
    username: str,
    password: str,
    host: str = "mongodb://127.0.0.1:",
    auth_source: Optional[str] = None,
    auth_mechanism: str = "SCRAM-SHA-1",
    timeout: int = 1_000,
) -> Database:
    """Returns a database handle."""
    try:
        auth_source = db_name if auth_source is None else auth_source
        client: MongoClient = MongoClient(
            host=f"{host}{port}",
            username=username,
            password=password,
            auth_source=auth_source,
            auth_mechanism=auth_mechanism,
            serverSelectionTimeoutMS=timeout,
        )
        dbh = client[db_name]
        return dbh
    except Exception as e:
        print(e)
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


def get_collection_list() -> list[str]:
    """Gets a list of the collections.

    NOTE: Because of the format of the config file this is a hardcoded list.
    """
    return [
        "biomarker_collection",
        "canonical_id_map_collection",
        "second_id_map_collection",
        "unreviewed_collection",
        "request_log_collection",
        "error_log_collection",
        "search_cache",
    ]
