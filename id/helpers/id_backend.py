""" Public facing entry point for ID backend processing/generating/assignment.

"""

from typing import Optional
from pymongo.database import Database
import os
import sys
from . import canonical_helpers as canonical
from . import second_level_helpers as second
import datetime
import deepdiff as dd
import re
import subprocess
from logging import Logger
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from tutils.logging import log_msg
from tutils.general import write_json
from tutils.constants import biomarker_default, unreviewed_default

CANONICAL_DEFAULT = canonical.CANONICAL_DEFAULT
SECOND_DEFAULT = second.SECOND_DEFAULT
DATA_DEFAULT = biomarker_default()
UNREVIEWED_DEFAULT = unreviewed_default()


def process_file_data(
    data: list,
    dbh: Database,
    filepath: str,
    logger: Logger,
    data_coll: str = DATA_DEFAULT,
    unreviewed_coll: str = UNREVIEWED_DEFAULT,
    can_id_coll: str = CANONICAL_DEFAULT,
    second_id_coll: str = SECOND_DEFAULT,
) -> list:
    """Processes the data for ID assignments.

    Parameters
    ----------
    data: list
        The list of data objects for the current file.
    dbh: Database
        The database handle.
    filepath: str
        The filepath of the current file being processed.
    logger: Logger
        The logger to use.
    data_coll: str
        The main data collection.
    unreviewed_coll: str
        The unreviewed data colelction.
    can_id_coll: str
        The canonical ID map collection.
    second_id_coll: str
        The second level ID map collection.

    Returns
    -------
    list
        Returns the updated data list with the ID values assigned.
    """
    if not data:
        log_msg(
            logger=logger,
            msg=f"No data found for `{filepath}`.",
            level="error",
            to_stdout=True,
        )
        return []

    updated_data: list[dict] = []
    collision_filepath = f"./collision_reports/{os.path.splitext(os.path.split(filepath)[1])[0]}_collisions.json"
    collisions: dict = {}
    # standard collisions are second level ID collisions where the new record differs somewhat from the
    # existing record
    standard_collision_count = 0
    # hard collisions are second level ID collisions where the new record matches the existing record
    # completely or is an entire subset of the existing record
    hard_collision_count = 0

    for idx, document in enumerate(data):

        canonical_id, second_level_id, second_level_collision, hash_value, core_str = (
            _id_assign(
                document=document,
                dbh=dbh,
                logger=logger,
                canonical_id_coll=can_id_coll,
                second_id_coll=second_id_coll,
            )
        )
        document["biomarker_canonical_id"] = canonical_id
        document["biomarker_id"] = second_level_id

        if second_level_collision:

            existing_record = get_record_by_id(second_level_id, False, dbh, data_coll)
            existing_unreviewed_records = get_record_by_id(
                second_level_id, False, dbh, unreviewed_coll
            )
            base_collision_obj = {
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "IDs": {"canonical": canonical_id, "second_level": second_level_id},
                "filepath": filepath,
                "hash_value": hash_value,
                "core_values_str": core_str,
            }
            reviewed_difference = dd.DeepDiff(
                document, existing_record, ignore_order=True
            ).to_json()
            unreviewed_object = None
            if existing_unreviewed_records is not None:
                unreviewed_differences = [
                    dd.DeepDiff(document, i).to_json()
                    for i in existing_unreviewed_records
                ]
                unreviewed_object = [
                    {f"collision_{idx}": json.loads(v)}
                    for idx, v in enumerate(unreviewed_differences)
                ]

            # hard collision
            if reviewed_difference == {}:
                hard_collision_count += 1
                _dict_key = f"HARD_COLLISION_NUM_{hard_collision_count}"
                collisions[_dict_key] = base_collision_obj
                collisions[_dict_key]["reviewed_collision_info"] = {
                    "collision_type": "hard",
                    "reviewed_difference": (
                        json.loads(reviewed_difference)
                        if existing_record
                        else "Collision with another record in current/."
                    ),
                }
                collisions[_dict_key]["unreviewed_collisions"] = (
                    unreviewed_object if unreviewed_object else []
                )
                output_message = (
                    f"HARD collision detected for record number `{idx}` on IDs"
                )
                output_message += (
                    f"`{canonical_id}`, `{second_level_id}` in file `{filepath}`."
                )
                document["collision"] = 2
            # soft collision
            else:
                standard_collision_count += 1
                _dict_key = f"STANDARD_COLLISION_NUM_{standard_collision_count}"
                collisions[_dict_key] = base_collision_obj
                collisions[_dict_key]["reviewed_collision_info"] = {
                    "collision_type": "soft",
                    "reviewed_difference": (
                        json.loads(reviewed_difference)
                        if existing_record
                        else "Collision with another record in current/."
                    ),
                }
                collisions[_dict_key]["unreviewed_collisions"] = (
                    unreviewed_object if unreviewed_object else []
                )
                output_message = (
                    f"STANDARD collision detected for record number `{idx}` on IDs "
                )
                output_message += (
                    f"`{canonical_id}`, `{second_level_id}` in file `{filepath}`."
                )
                document["collision"] = 1

            log_msg(logger=logger, msg=output_message, to_stdout=True)

        else:
            document["collision"] = 0

        updated_data.append(document)

    write_json(filepath=collision_filepath, data=collisions)
    log_str = f"Finished assigning IDs for {filepath}"
    log_str += f"\n\tSoft collisions: {standard_collision_count}"
    log_str += f"\n\tHard collisions: {hard_collision_count}"
    log_msg(logger=logger, msg=log_str)
    return updated_data


def _id_assign(
    document: dict,
    dbh: Database,
    logger: Logger,
    canonical_id_coll: str = CANONICAL_DEFAULT,
    second_id_coll: str = SECOND_DEFAULT,
) -> tuple:
    """Goes through the ID assign process for the passed document.

    Parameters
    ----------
    document: dict
        The document to assign the ID for.
    dbh: Database
        The database handle.
    logger: Logger
        The logger to use.
    can_id_coll: str (default: CANONICAL_DEFAULT)
        The name of the collection to check for hash collisions.
    second_id_coll: str (default: SECOND_DEFAULT)

    Returns
    -------
    tuple: (str, str, bool)
        The assigned canonical biomarker ID, second level ID, collision boolean, hash value, and core values string.
    """
    canonical_id, hash_value, core_values_str, canonical_collision = (
        canonical.get_ordinal_id(
            document=document, dbh=dbh, logger=logger, id_collection=canonical_id_coll
        )
    )
    second_level_id, second_level_collision = second.get_second_level_id(
        canonical_id=canonical_id,
        canonical_collision=canonical_collision,
        document=document,
        dbh=dbh,
        logger=logger,
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


def dump_id_collection(connection_string: str, save_path: str, collection: str) -> bool:
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

    Returns
    -------
    bool
        Indication if the collection was dumped successfully.
    """
    command = [
        "mongoexport",
        "--uri",
        connection_string,
        "--collection",
        collection,
        "--out",
        save_path,
    ]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        print("Args passed:")
        print(f"Connection string: {connection_string}")
        print(f"Save path: {save_path}")
        print(f"Collection: {collection}")
        print(e)
        return False
    return True
