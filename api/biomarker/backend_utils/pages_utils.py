""" Handles the backend logic for the pages endpoints.
"""

from typing import Tuple, Dict, List

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

    statistics = []
    database_stats = {"title": "Database Statistics"}
    database_stats_raw = {key.replace("_", " ").title(): val for key, val in stats.get("stats", {})}
    database_stats.update(database_stats_raw)
    statistics.append(database_stats)

    entity_type_splits = {"title": "Entity Types"}
    for split in stats.get("entity_type_splits", []):
        entity_type_splits[split["entity_type"]] = split["count"]
    statistics.append(entity_type_splits)

    return_object = {"statistics": statistics, "statistics_new": {}, "events": [], "video": {}}

    return return_object, 200


def ontology() -> Tuple[Dict | List, int]:
    """Entry point for the backend logic of returning the ontology JSON.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    return db_utils.get_ontology()
