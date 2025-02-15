""" Public facing entry point for ID backend processing/generating/assignment.

"""

from typing import Optional
from pymongo.database import Database
import os
import sys
from . import canonical_helpers as canonical
from . import second_level_helpers as second
from . import LOGGER
import re
from time import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from tutils.logging import log_msg
from tutils.constants import biomarker_default, unreviewed_default

CANONICAL_DEFAULT = canonical.CANONICAL_DEFAULT
SECOND_DEFAULT = second.SECOND_DEFAULT
DATA_DEFAULT = biomarker_default()
UNREVIEWED_DEFAULT = unreviewed_default()
LOG_CHECKPOINT = 5_000


def process_file_data(
    data: list,
    dbh: Database,
    filepath: str,
    can_id_coll: str = CANONICAL_DEFAULT,
    second_id_coll: str = SECOND_DEFAULT,
) -> list[dict]:
    """Processes the data for ID assignments.

    Parameters
    ----------
    data: list
        The list of data objects for the current file.
    dbh: Database
        The database handle.
    filepath: str
        The filepath of the current file being processed.
    can_id_coll: str, optional
        The canonical ID map collection.
    second_id_coll: str, optional
        The second level ID map collection.

    Returns
    -------
    list[dict]
        Returns the updated data list with the ID values assigned.
    """
    log_msg(logger=LOGGER, msg=f"Assigning IDs for `{filepath}`...", to_stdout=True)
    if not data:
        log_msg(
            logger=LOGGER,
            msg=f"No data found for `{filepath}`.",
            level="error",
            to_stdout=True,
        )
        return []

    updated_data: list[dict] = []

    start_time = time()
    collisions = 0
    new_biomarkers = 0
    for idx, document in enumerate(data):
        if (idx + 1) % LOG_CHECKPOINT == 0:
            log_msg(
                logger=LOGGER,
                msg=f"Hit log checkpoint on index: {idx + 1}",
                to_stdout=True,
            )

        if "collision" in document:
            del document["collision"]

        canonical_id, second_level_id, second_level_collision, _, _ = _id_assign(
            document=document,
            dbh=dbh,
            canonical_id_coll=can_id_coll,
            second_id_coll=second_id_coll,
        )
        document["biomarker_canonical_id"] = canonical_id
        document["biomarker_id"] = second_level_id

        if second_level_collision:
            document["collision"] = 1
            collisions += 1
        else:
            document["collision"] = 0
            new_biomarkers += 1

        updated_data.append(document)

    elapsed_time = time() - start_time
    msg = (
        f"Finished assigning IDs ({elapsed_time} seconds) for {filepath}\n"
        f"\tCollisions: {collisions}\n"
        f"\tNew biomarkers: {new_biomarkers}"
    )
    log_msg(logger=LOGGER, msg=msg, to_stdout=True)
    return updated_data


def _id_assign(
    document: dict,
    dbh: Database,
    canonical_id_coll: str = CANONICAL_DEFAULT,
    second_id_coll: str = SECOND_DEFAULT,
) -> tuple[str, str, bool, str, str]:
    """Goes through the ID assign process for a single document.

    Parameters
    ----------
    document: dict
        The document to assign the ID for.
    dbh: Database
        The database handle.
    can_id_coll: str (default: CANONICAL_DEFAULT)
        The name of the collection to check for hash collisions.
    second_id_coll: str (default: SECOND_DEFAULT)
        The name of the collection to check for second level collisions.

    Returns
    -------
    tuple: (str, str, bool, str, str)
        The assigned canonical biomarker ID, second level ID, collision boolean, hash value,
        and core values string.
    """
    canonical_id, hash_value, core_values_str, canonical_collision = (
        canonical.get_ordinal_id(
            document=document, dbh=dbh, id_collection=canonical_id_coll
        )
    )
    second_level_id, second_level_collision = second.get_second_level_id(
        canonical_id=canonical_id,
        canonical_collision=canonical_collision,
        document=document,
        dbh=dbh,
        id_collection=second_id_coll,
    )
    return (
        canonical_id,
        second_level_id,
        second_level_collision,
        hash_value,
        core_values_str,
    )


def get_record_by_id(
    biomarker_id: str, canonical_level: bool, dbh: Database, db_collection: str
) -> Optional[dict]:
    """Gets the record by the biomarker ID at the specified level.

    Parameters
    ----------
    biomarker_id: str
        The biomarker ID to search for.
    canonical_level: bool
        The level of the biomarker ID (True indicates canonical, False indicates second level ID).
    dbh: Database
        The database handle.
    db_collection:
        The database collection.

    Returns
    -------
    dict or None
        The biomarker record or None if nothing is found.
    """
    search_field = "biomarker_canonical_id" if canonical_level else "biomarker_id"
    record = dbh[db_collection].find_one({search_field: biomarker_id}, {"_id": 0})
    return record


def validate_id_format(id: str, level: int) -> bool:
    """Validates the format of the ID.

    Parameters
    ----------
    id: str
        The ID to validate.
    level: int
        The level of the ID (0 for canonical, 1 for second level).

    Returns
    -------
    bool
        True for correct validation, False on failure to validate.
    """
    if level == 0:
        if re.match(r"[A-Z]{2}\d{4}", id) is None:
            return False
        return True
    elif level == 1:
        if re.match(r"[A-Z]{2}\d{4}-\d", id) is None:
            return False
        return True
    else:
        print(f"Invalid level value `{level}` passed to validate_id_format.")
        return False
