"""Core database interaction functions for MongoDB."""

import datetime
import hashlib
import json
import logging
import random
import string
from pprint import pformat
import traceback
from typing import Optional, Dict, cast, Tuple, List, Any, Literal
from typing_extensions import deprecated

import pytz
from flask import current_app, Flask, Request
from pymongo.errors import PyMongoError
from user_agents import parse

from . import (
    ERROR_LOG_COLLECTION,
    TIMESTAMP_FORMAT,
    TIMEZONE,
    DB_COLLECTION,
    SEARCH_CACHE_COLLECTION,
    STATS_COLLECTION,
    VERSION_COLLECTION,
    ONTOLOGY_COLLECTION,
    REQ_LOG_COLLECTION,
    REQ_LOG_MAX_LEN,
    CustomFlask,
)

# --- Utility Functions ---


def create_timestamp() -> str:
    """Creates a standardized timestamp string for the configured timezone.

    Returns
    -------
    str
        The current timestamp as a string.
    """
    timestamp = datetime.datetime.now(pytz.timezone(TIMEZONE)).strftime(
        TIMESTAMP_FORMAT
    )
    return timestamp


def cast_app(app: Flask) -> CustomFlask:
    """Casts the Flask app as the CustomFlask instance for static type
    checking. Allows accessing custom atributes like `mongo_db` and
    `api_logger` with type safety.

    Parameters
    ----------
    app: Flask
        The Flask current_app instance.

    Returns
    -------
    CustomFlask
        The casted current_app instance.
    """
    custom_app = cast(CustomFlask, app)
    return custom_app


def _create_error_obj(error_id: str, error_msg: str, **kwargs: Any) -> Dict[str, Any]:
    """Creates a standardized object for API error responses.

    Parameters
    ----------
    error_id: str
        A unique identifier for this specific error occurrence.
    error_msg: str
        A short, standardized error code or message.
    **kwargs
        Additional key-value pairs to include in the error object.

    Returns
    -------
    dict[str, Any]
        A dictionary representing the error response.
    """
    error_object: Dict[Any, Any] = {
        "error": {
            "error_id": error_id,
            "error_msg": error_msg,
        }
    }
    if kwargs:
        error_object["error"].update(kwargs)
    return error_object


# --- Error Logging ----


def log_error(error_log: str, error_msg: str, origin: str, **kwargs) -> Dict:
    """Logs an error to the dedicated MongoDB error collection and the app logger.

    Parameters
    ----------
    error_log : str
        The error message to log (a traceback stack trace or custom error message).
    error_msg : str
        A user facing, standardized short error message.
    origin : str
        The function calling this function.
    **kwargs
        Additional context to include in the standardized error response.

    Returns
    -------
    dict
        The return JSON.
    """
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db

    def _create_error_id(
        size: int = 6, chars: str = string.ascii_uppercase + string.digits
    ) -> str:
        """Creates a short random alphanumeric ID for the error."""
        return "".join(random.choice(chars) for _ in range(size))

    error_id = _create_error_id()
    error_object_to_log = {
        "id": error_id,
        "log": error_log,
        "msg": error_msg,
        "origin": origin,
        "timestamp": create_timestamp(),
    }

    try:
        dbh[ERROR_LOG_COLLECTION].insert_one(error_object_to_log)
        custom_app.api_logger.error(
            f"Error logged (ID: {error_id}): msg=`{error_msg}`, origin=`{origin}`\ndetails=`{error_log}`\nkwargs={pformat(kwargs)}"
        )
    except Exception as e:
        custom_app.api_logger.critical(
            f"CRITICAL: Failed to log error to MongoDB.\nOriginal Error Object: {error_object_to_log}\nLogger Error: {e}\n{traceback.format_exc()}"
        )

    return _create_error_obj(error_id, error_msg, **kwargs)


# --- Database Query Functions ---


def find_one(
    query_object: Dict,
    projection_object: Dict = {"_id": 0, "all_text": 0},
    collection: str = DB_COLLECTION,
) -> Tuple[Dict, int]:
    """Performs a find_one query on the specified collection.

    Parameters
    ----------
    query_object: dict
        The MongoDB query filter document.
    projection_object: dict, optional
        The MongoDB projection document. Defaults to excluding `_id` and `all_text` fields.
    collection: str, optional
        The collection to query.

    Returns
    -------
    tuple: (dict, int)
        The retrieved document or error object and the HTTP status code.
    """
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db

    try:
        result = dbh[collection].find_one(query_object, projection_object)

        if result is None:
            error_obj = log_error(
                error_log=f"Document not found. Query: {query_object}, Projection: {projection_object}, Collection: {collection}",
                error_msg="record-not-found",
                origin="find_one",
            )
            return error_obj, 404
        else:
            return result, 200
    except PyMongoError as db_error:
        error_obj = log_error(
            error_log=f"PyMongoError during find_one. Query: {query_object}, Collection: {collection}.\nError: {db_error}\n{traceback.format_exc()}",
            error_msg="internal-database-error",
            origin="find_one",
        )
        return error_obj, 500
    except Exception as e:
        error_obj = log_error(
            error_log=f"Unexpected error during find_one. Query: {query_object}, Collection: {collection}.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-server-error",
            origin="find_one",
        )
        return error_obj, 500


def execute_pipeline(
    pipeline: List, collection: str = DB_COLLECTION, disk_use: bool = True
) -> Tuple[Dict, int]:
    """Executes a MongoDB aggregation framework pipeline.

    Parameters
    ----------
    pipeline: list
        The list of aggregation pipeline stages.
    collection: str, optional
        The collection to run the pipeline against.
    disk_use: bool, optional
        Whether to allow MongoDB to use disk for larger pipelines.

    Returns
    -------
    tuple: (dict, int)
        The result of the pipeline execution or error object and the HTTP status code.
    """
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db

    try:

        custom_app.api_logger.debug(
            "********************************** Pipeline Log **********************************"
        )
        custom_app.api_logger.debug(f"PIPELINE:\n{pipeline}\n")
        if custom_app.api_logger.level == logging.DEBUG:
            explain_output = dbh.command(
                "aggregate", collection, pipeline=pipeline, explain=True
            )
            custom_app.api_logger.info(f"COMMAND EXPLAIN OUTPUT:\n{explain_output}\n")

        cursor = dbh[collection].aggregate(pipeline, allowDiskUse=disk_use)
        result = next(cursor)

        return result, 200
    except PyMongoError as db_error:
        error_obj = log_error(
            error_log=f"PyMongoError during aggregate. Pipeline: {pipeline}, Collection: {collection}.\nError: {db_error}\n{traceback.format_exc()}",
            error_msg="internal-database-error",
            origin="execute_pipeline",
        )
        return error_obj, 500
    except Exception as e:
        error_obj = log_error(
            error_log=f"Unexpected error during aggregate. Pipeline: {pipeline}, Collection: {collection}.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-server-error",
            origin="execute_pipeline",
        )
        return error_obj, 500


# --- Database Caching Logic ---
# Note: This isn't really a real cache, this structure was built in order to match the GlyGen API search workflow, ideally
# this is completely removed eventually.


def _get_query_hash(query_object: Dict) -> str:
    """Generates an MD5 hash for a given query object to use as a cache key."""
    hash_string = json.dumps(query_object, sort_keys=True)
    hash_hex = hashlib.md5(hash_string.encode("utf-8")).hexdigest()
    return hash_hex


def _search_cache(
    list_id: str, cache_collection: str = SEARCH_CACHE_COLLECTION
) -> Tuple[bool, Optional[Dict]]:
    """Checks if the list id (query hash) exists in the cache collection.

    Parameters
    ----------
    list_id: str
        The list id (the MD5 query hash) of the query object.
    cache_collection: str, optional
        The cache collection.

    Returns
    -------
    tuple: (bool, Dict or None)
        Boolean whether the list_id exists in the cache and None on success, or an error object.
    """
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db
    list_id_query = {"list_id": list_id}

    try:
        count = dbh[cache_collection].count_documents(list_id_query, limit=1)
        return (True, None) if count > 0 else (False, None)
    except PyMongoError as e:
        error_object = log_error(
            error_log=f"PyMongoError checking cache for list_id `{list_id}` in `{cache_collection}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-database-error",
            origin="_search_cache",
        )
        return (False, error_object)
    except Exception as e:
        error_object = log_error(
            error_log=f"Unexpected error checking cache for list_id `{list_id}` in `{cache_collection}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-server-error",
            origin="_search_cache",
        )
        return (False, error_object)


def _cache_object(
    list_id: str,
    request_arguments: Dict,
    query_object: Dict,
    search_type: Literal["simple", "full"],
    cache_collection: str = SEARCH_CACHE_COLLECTION,
    ai_search_metadata: Optional[Dict] = None,
) -> Tuple[Dict, int]:
    """Stores a search query and its associated request arguments in the cache collection. It also first deletes
    any existing cache entry with the same list_id.

    Parameters
    ----------
    list_id: str
        The MD5 hash of the query object.
    request_arguments: dict
        The original API request arguments.
    query_object: dict
        The generated MongoDB query.
    search_type: "simple" or "full"
        The type of search performed.
    cache_collection: str, optional
        The cache collection.

    Returns
    -------
    tuple : (dict, int)
        The return object or error object and HTTP status code.
    """
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db

    cache_object_to_insert = {
        "list_id": list_id,
        "cache_info": {
            "api_request": request_arguments,
            "query": query_object,
            "search_type": search_type,
            "timestamp": create_timestamp(),
        },
    }
    if ai_search_metadata is not None:
        cache_object_to_insert["cache_info"]["ai_parsing"] = ai_search_metadata  # type: ignore

    try:
        dbh[cache_collection].delete_many({"list_id": list_id})
        dbh[cache_collection].insert_one(cache_object_to_insert)

        return {"list_id": list_id}, 200
    except PyMongoError as e:
        error_object = log_error(
            error_log=f"PyMongoError caching object for list_id `{list_id}` in `{cache_collection}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-database-error",
            origin="_cache_object",
        )
        return error_object, 500
    except Exception as e:
        error_object = log_error(
            error_log=f"Unexpected error caching object for list_id `{list_id}` in `{cache_collection}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-server-error",
            origin="_cache_object",
        )
        return error_object, 500


def search_and_cache(
    request_object: Dict,
    query_object: Dict,
    search_type: Literal["simple", "full"],
    cache_collection: str = SEARCH_CACHE_COLLECTION,
    ai_search_metadata: Optional[Dict] = None,
) -> Tuple[Dict[Any, Any], int]:
    """Checks cache for a query; if not found caches the query info.

    Function orchestrates the caching process:
        1. Hashes the `query_object` to get a `list_id`
        2. Checks if `list_id` exists in the `cache_collection` using `_search_cache`
        3. If not cached, calls `_cache_object` to store the `request_object` and `query_object`
        4. Returns the `list_id`

    Note: This two-step process with the list_id and query hashing is legacy code
    and to stay inline with the GlyGen API workflow. This should eventually
    be removed.

    Parameters
    ----------
    request_object: dict,
        The original API request arguments.
    query_object: dict
        The MongoDB query generated from the request.
    search_type: "simple" or "full"
        The search type, either simple or full.
    cache_collection: str, optional
        The cache collection.

    Returns
    -------
    tuple: (dict, int)
        The return object or an error object and HTTP status code.
    """
    dict_to_hash = (
        {**query_object, **ai_search_metadata}
        if ai_search_metadata is not None
        else query_object
    )
    list_id = _get_query_hash(dict_to_hash)
    cache_hit, error_object = _search_cache(list_id, cache_collection)

    if error_object is not None:
        return error_object, 500

    if not cache_hit:
        return_object, http_code = _cache_object(
            list_id,
            request_object,
            query_object,
            search_type,
            cache_collection,
            ai_search_metadata,
        )
        if http_code != 200:
            return return_object, http_code

    return {"list_id": list_id}, 200


def get_cached_objects(
    request_object: Dict,
    query_object: Dict,
    projection_object: Dict = {"_id": 0},
    cache_collection: str = SEARCH_CACHE_COLLECTION,
) -> Tuple[Dict, int]:
    """Retrieves cached search query information using a list_id.

    Parameters
    ----------
    request_object: dict
        The original API request (used for logging on error).
    query_object: dict
        The MongoDB query to find the cache entry.
    projection_object: dict, optional
        The projection for the cache entry. Defaults to excluding the `_id"` field.
    cache_collection: str, optional
        The cache collection.

    Returns
    -------
    tuple : (dict, int)
        The cached query object or error object and HTTP status code.
    """
    try:
        cache_entry = find_one(query_object, projection_object, cache_collection)
        cache_data, http_status = cache_entry

        if http_status != 200:
            # Handle `record-not-found` (404) or database errors (500)
            if http_status == 404:
                error_obj = log_error(
                    error_log=f"User search on non-existent list id. Query: {query_object}, Request: {request_object}",
                    error_msg="non-existent-search-results",
                    origin="get_cached_objects",
                )
                return error_obj, 404
            else:
                return cache_data, http_status
        else:
            # Success, format the return object
            if (
                "cache_info" not in cache_data
                or "query" not in cache_data["cache_info"]
            ):
                error_obj = log_error(
                    error_log=f"Malformed cache entry found. Query: {query_object}, Entry: {cache_data}",
                    error_msg="internal-server-error",
                    origin="get_cached_objects",
                )
                return error_obj, 500

            return {
                "mongo_query": cache_data["cache_info"]["query"],
                "cache_info": cache_data["cache_info"],
            }, 200

    except Exception as e:
        error_object = log_error(
            error_log=f"Unexpected error in retrieving cached object. Query: {query_object}, Request: {request_object}.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-server-error",
            origin="get_cached_objects",
        )
        return error_object, 500


# --- Metadata / Stats Functions ---


def get_version(version_collection: str = VERSION_COLLECTION) -> Tuple[Dict, int]:
    """Retrieves API and data version information from the version collection."""
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db

    try:
        version_cursor = dbh[version_collection].find(
            {"component": {"$in": ["api", "data"]}}, {"_id": 0}
        )
        versions_list = list(version_cursor)
        return_data = {"version": versions_list}
        return return_data, 200
    except PyMongoError as e:
        error_object = log_error(
            error_log=f"PymongoError querying for version info in `{version_collection}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-database-error",
            origin="get_version",
        )
        return error_object, 500
    except Exception as e:
        error_object = log_error(
            error_log=f"Unexpected error in querying for version info in `{version_collection}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-database-error",
            origin="get_version",
        )
        return error_object, 500


def get_stats(
    mode: Literal["stats", "split", "both"] = "both",
    stat_collection: str = STATS_COLLECTION,
) -> Tuple[Dict, int]:
    """Retrieves statistics (counts, entity type splits) from the stats collection.

    Parameters
    ----------
    mode: "stats", "split", "both"
        Specifies which stats to retrieve.
    stat_collection: str, optional
        The stat collection to retrieve from.

    Returns
    -------
    tuple: (dict, int)
        The requested stat object or error object and HTTP status code.
    """
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db
    data: Dict = {}

    try:
        if mode in ["stats", "both"]:
            stats = dbh[stat_collection].find_one({"_id": "stats"}, {"_id": 0})
            data["stats"] = stats if stats else {}
        if mode in ["split", "both"]:
            splits = dbh[stat_collection].find_one(
                {"_id": "entity_type_splits"}, {"_id": 0}
            )
            data["entity_type_splits"] = splits["splits"] if splits else []

        return data, 200

    except PyMongoError as e:
        error_object = log_error(
            error_log=f"PymongoError querying for stats in `{stat_collection}`, Mode: `{mode}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-database-error",
            origin="get_stats",
        )
        return error_object, 500
    except Exception as e:
        error_object = log_error(
            error_log=f"Unexpected error in querying for stats in `{stat_collection}`, Mode: `{mode}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-server-error",
            origin="get_stats",
        )
        return error_object, 500


def get_ontology(
    ontology_collection: str = ONTOLOGY_COLLECTION, filter_nulls: bool = True
) -> Tuple[List | Dict, int]:
    """Retrieves the ontology data structure.

    Parameters
    ----------
    ontology_collection: str, optional
        The ontology collection to retrieve from.
    filter_nulls: bool, optional
        Whether to filter nodes with null id values.

    Returns
    -------
    tuple: (list | dict, int)
        The ontology data (list) or an error object (dict) and the HTTP status code.
    """
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db

    try:
        ontology_json = dbh[ontology_collection].find_one({}, {"_id": 0})

        if ontology_json is None or "data" not in ontology_json:
            error_obj = log_error(
                error_log=f"Ontology document or 'data' key not found in collection `{ontology_collection}`.",
                error_msg="ontology-data-not-found",
                origin="get_ontology",
            )
            return error_obj, 404

        ontology_data = ontology_json["data"]

        if filter_nulls:
            filtered_data = [
                item for item in ontology_data if item.get("id") is not None
            ]
            return filtered_data, 200
        else:
            return ontology_data, 200

    except PyMongoError as db_error:
        error_obj = log_error(
            error_log=f"PyMongoError querying for ontology in `{ontology_collection}`.\nError: {db_error}\n{traceback.format_exc()}",
            error_msg="internal-database-error",
            origin="get_ontology",
        )
        return error_obj, 500
    except Exception as e:
        error_obj = log_error(
            error_log=f"Unexpected error querying for ontology in `{ontology_collection}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="internal-server-error",
            origin="get_ontology",
        )
        return error_obj, 500


# --- Deprecated ---


@deprecated("SQLite is used for logging now to reduce backend overhead")
def log_request(
    request_object: Optional[Dict], endpoint: str, api_request: Request
) -> Optional[Dict[Any, Any]]:
    """Logs an API request in the request log collection.

    Parameters
    ----------
    request_object : dict or None
        The parsed query string parameters associated with the API call (if available).
    endpoint : str
        The endpoint the request came in for.
    api_request : Request
        The flask request object.

    Returns
    -------
    dict or None
        None on success, error object on error.
    """
    if request_object and len(json.dumps(request_object)) > REQ_LOG_MAX_LEN:
        error_obj = log_error(
            error_log=f"Request object length exceeds REQ_LOG_MAX_LEN ({REQ_LOG_MAX_LEN})",
            error_msg="request-object-exceeded-max-length",
            origin="log_request",
        )
        return error_obj
    header_dict = {
        "user_agent": api_request.headers.get("User-Agent"),
        "referer": api_request.headers.get("Referer"),
        "origin": api_request.headers.get("Origin"),
        "ip": api_request.environ.get("HTTP_X_FORWARDED_FOR", api_request.remote_addr),
    }
    user_agent = parse(api_request.headers.get("User-Agent"))
    header_dict["is_bot"] = user_agent.is_bot
    log_object = {
        "api": endpoint,
        "request": request_object,
        "timestamp": create_timestamp(),
        "headers": header_dict,
    }
    custom_app = cast_app(current_app)
    dbh = custom_app.mongo_db
    try:
        dbh[REQ_LOG_COLLECTION].insert_one(log_object)
    except Exception as e:
        error_obj = log_error(
            error_log=f"Failed to log request.\n{e}",
            error_msg="log-failure",
            origin="log_request",
        )
        return error_obj
    return None
