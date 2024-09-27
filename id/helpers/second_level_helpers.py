""" Handles all the logic for assigning/generating the second level ID.
"""

import sys
import os
from logging import Logger
from pymongo.database import Database
from typing import NoReturn, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from tutils.logging import log_msg
from tutils.constants import second_level_id_default

SECOND_DEFAULT = second_level_id_default()


def get_second_level_id(
    canonical_id: str,
    canonical_collision: bool,
    document: dict,
    dbh: Database,
    logger: Logger,
    id_collection: str = SECOND_DEFAULT,
) -> tuple[str, bool]:
    """Assigns the second level ID to the document.

    Parameters
    ----------
    canonical_id: str
        The canonical ID for the document.
    canonical_collision: bool
        Whether there was a collision for the canonical ID.
    document: dict
        The document to assign the second level ID for.
    dbh: Database
        The database handle.
    logger: Logger
        The logger to use.
    id_collection: str (default: SECOND_DEFAULT)
        The second level ID map collection.

    Returns
    -------
    tuple: (str, bool)
        The assigned second level ID and a collision flag.
    """
    second_level_key = _get_key(document=document, logger=logger)
    collision_status = False
    if canonical_collision:
        collision_status = _check_collision(
            canonical_id=canonical_id,
            key=second_level_key,
            dbh=dbh,
            logger=logger,
            id_collection=id_collection,
        )
    second_level_id = _assign_ordinal(
        canonical_id=canonical_id,
        collision=collision_status,
        key=second_level_key,
        dbh=dbh,
        logger=logger,
        id_collection=id_collection,
    )
    return second_level_id, collision_status


def _assign_ordinal(
    canonical_id: str,
    collision: bool,
    key: str,
    dbh: Database,
    logger: Logger,
    id_collection: str = SECOND_DEFAULT,
) -> str:
    """Assigns the ordinal second level ID.

    Parameters
    ----------
    canonical_id: str
        The canonical biomarker ID.
    collision: bool
        Whether or not there is a collision on the second level ID.
    key: str
        The condition_id or exposure_agent_id to use for the existing entry key.
    dbh: Database
        The database handle.
    logger: Logger
        The logger to use.
    id_collection: str (default: SECOND_DEFAULT)
        The ID collection map.

    Returns
    -------
    str
        The assigned ordinal second level ID.
    """
    if collision:
        second_level_id = _get_ordinal_id(
            canonical_id=canonical_id,
            key=key,
            dbh=dbh,
            logger=logger,
            id_collection=id_collection,
        )
        return second_level_id
    second_level_id = _new_ordinal(
        canonical_id=canonical_id, key=key, dbh=dbh, id_collection=id_collection
    )
    return second_level_id


def _get_ordinal_id(
    canonical_id: str,
    key: str,
    dbh: Database,
    logger: Logger,
    id_collection: str = SECOND_DEFAULT,
) -> str | NoReturn:
    """Gets the existing corresponding second level ID for the key.

    Parameters
    ----------
    canonical_id: str
        The canonical ID to search under.
    key: str
        The key to query the existing entries on.
    dbh: Database
        The database handle.
    logger: Logger
        The logger to use.
    id_collection: str
        The ID collection map.

    Returns
    -------
    str
        The corresponding ordinal second level ID.
    """
    existing_entries = _get_existing_entries(
        canonical_id=canonical_id, dbh=dbh, id_collection=id_collection
    )
    if not existing_entries:
        log_str = f"Some error occurred in looking up existing ordinal second level ID in `{id_collection}` for:"
        log_str += f"\n\tcanonical ID: `{canonical_id}`"
        log_str += f"\n\tkey: `{key}`"
        log_msg(logger=logger, msg=log_str, level="error", to_stdout=True)
        sys.exit(1)
    for entry in existing_entries:
        if key in entry.keys():
            return entry[key]
    log_str = "Did not find existing second level ID despite expecting collision."
    log_str += f"\n\tcanonical ID: `{canonical_id}`"
    log_str += f"\n\tkey: `{key}`"
    log_str += f"\n\texisting entries: `{existing_entries}`"
    log_msg(logger=logger, msg=log_str, level="error", to_stdout=True)
    sys.exit(1)


def _new_ordinal(
    canonical_id: str, key: str, dbh: Database, id_collection: str = SECOND_DEFAULT
) -> str:
    """Adds a new entry to the canonical ID record and increments the curr_index. Exits on error.

    Parameters
    ----------
    canonical_id: str
        The canonical ID to create a new entry under.
    key: str
        The condition or exposure agent key for the new entry.
    dbh: Database
        The database handle.
    id_collection: str (default: SECOND_DEFAULT)

    Return
    ------
    str
        The new incremented ordinal second level ID that was generated and added.
    """
    record_to_update = _get_canonical_map(canonical_id, dbh, id_collection)
    if record_to_update is None:
        second_level_id = f"{canonical_id}-1"
        new_entry = {
            "biomarker_canonical_id": canonical_id,
            "values": {"curr_index": 1, "existing_entries": [{key: second_level_id}]},
        }
        dbh[id_collection].insert_one(new_entry)
    else:
        new_index = record_to_update["values"]["curr_index"] + 1
        second_level_id = f"{canonical_id}-{new_index}"
        dbh[id_collection].update_one(
            {"biomarker_canonical_id": canonical_id},
            {
                "$set": {"values.curr_index": new_index},
                "$push": {"values.existing_entries": {key: second_level_id}},
            },
        )
    return second_level_id


def _check_collision(
    canonical_id: str,
    key: str,
    dbh: Database,
    logger: Logger,
    id_collection: str = SECOND_DEFAULT,
) -> bool:
    """Checks for a second level collision.

    Parameters
    ----------
    canonical_id: str
        The canonical ID for the document.
    key: str
        The condition_id or exposure_agent_id values to compare on.
    dbh: Database
        The database handle.
    logger: Logger
        The logger to use.
    id_collection: str (default: SECOND_DEFAULT)

    Returns
    -------
    bool
        The collision flag.
    """
    existing_entries = _get_existing_entries(canonical_id, dbh, id_collection)
    if existing_entries is None:
        log_str = "Unexpected error when querying for existing biomarker canonical ID existing entries:"
        log_str += "\n\tcanonical ID: `{canonical_id}`"
        log_str += "\n\tID collection: `{id_collection}`"
        log_msg(logger=logger, msg=log_str, level="error", to_stdout=True)
        sys.exit(1)
    existing_keys = [
        existing_key for entry in existing_entries for existing_key in entry.keys()
    ]
    return key in existing_keys


def _get_existing_entries(
    canonical_id: str, dbh: Database, id_collection: str = SECOND_DEFAULT
) -> Optional[list]:
    """Shortcut to get the existing entries from a canonical ID record.

    Parameters
    ----------
    canonical_id: str
        the canonical ID to search on.
    dbh: Database
        The database handle.
    id_collection: str (default: SECOND_DEFAULT)
        The ID map collection
    """
    record = _get_canonical_map(canonical_id, dbh, id_collection)
    if record is None:
        return None
    return record["values"]["existing_entries"]


def _get_canonical_map(
    canonical_id: str, dbh: Database, id_collection: str = SECOND_DEFAULT
) -> Optional[dict]:
    """Gets the canonical ID data record.

    Parameters
    ----------
    canonical_id: str
        The canonical ID for the document.
    dbh: Database
        The database handle.
    id_collection: str (default: SECOND_DEFAULT)
        The ID map collection

    Returns
    -------
    dict or None
        The canonical ID data record. None if nothing was found.
    """
    return dbh[id_collection].find_one({"biomarker_canonical_id": canonical_id})


def _get_key(document: dict, logger: Logger) -> str:
    """Gets the key to compare against for second level ID generation.

    Parameters
    ----------
    document: dict
        The document to extract the key from.
    logger: Logger
        The logger to use.

    Returns
    -------
    str
        Either the condition ID or the exposure agent ID
    """
    key1 = "condition"
    key2 = "exposure_agent"
    if key1 in document.keys() and document[key1] is not None and document[key1]["id"]:
        return document[key1]["id"]
    elif (
        key2 in document.keys() and document[key2] is not None and document[key2]["id"]
    ):
        return document[key2]["id"]
    else:
        log_msg(
            logger=logger,
            msg=f"Error when parsing document for second level ID key.\nDocument: {document}",
            level="error",
            to_stdout=True,
        )
        sys.exit(1)
