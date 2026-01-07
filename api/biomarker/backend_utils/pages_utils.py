"""Handles the backend logic for the pages endpoints."""

from typing import Tuple, Dict, List
import pytz
import datetime
import pymongo

from flask import current_app

from . import EVENT_COLLECTION
from . import db as db_utils
from . import utils as utils


def home_init() -> Tuple[Dict, int]:
    """Entry point for the backend logic of the home init endpoint.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    stats, stats_http_code = db_utils.get_stats(mode="both")
    if stats_http_code != 200:
        return stats, stats_http_code

    versions, version_http_code = db_utils.get_version()
    if version_http_code != 200:
        return versions, version_http_code

    statistics = []
    database_stats = {"title": "Database Statistics"}
    database_stats_raw = {
        key.replace("count", "").replace("_", " ").title(): val
        for key, val in stats.get("stats", {}).items()
    }
    database_stats.update(database_stats_raw)
    statistics.append(database_stats)

    entity_type_splits = {"title": "Entity Types"}
    for split in stats.get("entity_type_splits", []):
        entity_type = split["entity_type"]
        entity_type = (
            entity_type
            if entity_type in {"miRNA", "mRNA", "RNA", "DNA"}
            else entity_type.title()
        )
        entity_type_splits[entity_type] = split["count"]
    statistics.append(entity_type_splits)

    # Get events
    custom_app = db_utils.cast_app(current_app)
    dbh = custom_app.mongo_db
    events = []
    try:
        # Calculate current time in seconds
        now_est = datetime.datetime.now(pytz.timezone("US/Eastern")).strftime(
            "%m/%d/%Y %H:%M:%S"
        )
        dt, tm = now_est.split(" ")[0], now_est.split(" ")[1]
        mm, dd, yy = dt.split("/")
        hr, mn, sc = tm.split(":")
        now_in_seconds = (
            int(yy) * 365 * 24 * 3600
            + int(mm) * 31 * 24 * 3600
            + int(dd) * 24 * 3600
            + int(hr) * 3600
            + int(mn) * 60
            + int(sc)
        )

        # Build query conditions
        cond_list = [
            {"visibility": {"$eq": "visible"}},
            {"start_date_s": {"$lte": now_in_seconds}},
            {"end_date_s": {"$gte": now_in_seconds}},
        ]
        query = {"$and": cond_list}

        doc_list = (
            dbh[EVENT_COLLECTION].find(query).sort("createdts", pymongo.DESCENDING)
        )

        # Process results
        for doc in doc_list:
            # Convert _id to string
            doc["id"] = str(doc["_id"])
            doc.pop("_id")

            # Format datetime fields
            for k in ["createdts", "updatedts", "start_date", "end_date"]:
                if k in doc and hasattr(doc[k], "strftime"):
                    doc[k] = doc[k].strftime("%Y-%m-%d %H:%M:%S %Z%z")

            # Add timestamp for now
            doc["now_in_seconds"] = now_in_seconds
            events.append(doc)
    except Exception as e:
        custom_app.api_logger.error(f"Error retrieving events: {str(e)}")

    return_object = {
        "version": versions["version"],
        "statistics": statistics,
        "statistics_new": {},
        "events": events,
        "video": {},
    }

    return return_object, 200


def ontology() -> Tuple[Dict | List, int]:
    """Entry point for the backend logic of returning the ontology JSON.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    return db_utils.get_ontology()
