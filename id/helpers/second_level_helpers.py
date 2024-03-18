''' Handles all the logic for assigning/generating the second level ID.

Public functions:
    get_second_level_id: The entry point for the second level ID assignment process.

Private functions:

'''

import logging
import sys
import pymongo
from pymongo.database import Database
from typing import Union
import misc_functions as misc_fn
import deepdiff as dd #type: ignore

SECOND_DEFAULT = 'second_id_map_collection'

def get_second_level_id(canonical_id: str, 
                        canonical_collision: bool, 
                        document: dict, 
                        dbh: Database, 
                        id_collection: str = SECOND_DEFAULT) -> tuple[str, bool]:
    ''' Assigns the second level ID to the document.

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
    id_collection: str (default: SECOND_DEFAULT)
        The second level ID map collection.

    Returns
    -------
    tuple: (str, bool)
        The assigned second level ID and a collision flag.
    '''
    second_level_key = _get_key(document)
    collision_status = False
    if canonical_collision:
        collision_status = _check_collision(canonical_id, second_level_key, dbh, id_collection)
    second_level_id = _assign_ordinal(canonical_id, collision_status, second_level_key, dbh, id_collection)

def _assign_ordinal(canonical_id: str, collision: bool, key: str, dbh: Database, id_collection: str = SECOND_DEFAULT) -> str:
    ''' Assigns the ordinal second level ID.

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
    id_collection: str (default: SECOND_DEFAULT)
        The ID collection map.

    Returns
    -------
    str
        The assigned ordinal second level ID.
    '''
    if collision:
        second_level_id = _get_ordinal_id(canonical_id, key, dbh, id_collection)
        return second_level_id
    second_level_id = _new_ordinal()
    return second_level_id

def _get_ordinal_id(canonical_id: str, key: str, dbh: Database, id_collection: str = SECOND_DEFAULT) -> str:
    ''' Gets the existing corresponding second level ID for the key. Will exit on Unexpected error.

    Parameters
    ----------
    canonical_id: str
        The canonical ID to search under.
    key: str
        The key to query the existing entries on.
    dbh: Database
        The database handle.
    id_collection: str
        The ID collection map.

    Returns
    -------
    str
        The corresponding ordinal second level ID.
    '''
    existing_entries = _get_existing_entries(canonical_id, dbh, id_collection)
    if not existing_entries:
        logging.error(
            f'Some error occurred in looking up existing ordinal second level ID in `{id_collection}` for:\n\
            \tcanonical ID: `{canonical_id}`\n\
            \tkey: `{key}`'
        )
        sys.exit(1)
    for entry in existing_entries:
        if key in entry.keys():
            return entry[key]
    logging.error(
        f'Did not find existing second level ID despite expecting collision.\n\
        \tcanonical ID: `{canonical_id}`\n\
        \tkey: `{key}`\n\
        \texisting entries: `{existing_entries}`'
    )
    sys.exit(1)

def _new_ordinal(canonical_id: str, key: str, dbh: Database, id_collection: str = SECOND_DEFAULT) -> str:
    ''' Adds a new entry to the canonical ID record and increments the curr_index. Exits on error.

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
    '''
    record_to_update = _get_canonical_map(canonical_id, dbh, id_collection)
    if record_to_update is None:
        second_level_id = f'{canonical_id}-1'
        new_entry = {
            'biomarker_canonical_id': canonical_id,
            'values': {
                "curr_index": 1,
                "existing_entries": [
                    {
                        key: second_level_id
                    }
                ]
            }
        }
        dbh[id_collection].insert_one(new_entry)
    else:
        curr_index = record_to_update['values']['curr_index']
        second_level_id = f'{canonical_id}-{curr_index + 1}'
        # TODO impelement update record and return 
    return second_level_id

def _check_collision(canonical_id: str, key: str, dbh: Database, id_collection: str = SECOND_DEFAULT) -> bool:
    ''' Checks for a second level collision.

    Parameters
    ----------
    canonical_id: str
        The canonical ID for the document.
    key: str
        The condition_id or exposure_agent_id values to compare on.
    dbh: Database
        The database handle.
    id_collection: str (default: SECOND_DEFAULT)

    Returns
    -------
    bool
        The collision flag.
    '''
    existing_entries = _get_existing_entries(canonical_id, dbh, id_collection)
    if existing_entries is None:
        logging.error(
            f'Unexpected error when querying for existing biomarker canonical ID existing entries:\n\
            \tcanonical ID: `{canonical_id}`\n\
            \tID collection: `{id_collection}`'
        )
        sys.exit(1)
    existing_keys = [existing_key for entry in existing_entries for existing_key in entry.keys()]
    return key in existing_keys

def _get_existing_entries(canonical_id: str, dbh: Database, id_collection: str = SECOND_DEFAULT) -> Union[list, None]:
    ''' Shortcut to get the existing entries from a canonical ID record.

    Parameters
    ----------
    canonical_id: str
        the canonical ID to search on.
    dbh: Database
        The database handle.
    id_collection: str (default: SECOND_DEFAULT)
        The ID map collection
    '''
    record = _get_canonical_map(canonical_id, dbh, id_collection)
    if record is None:
        return None
    return record['values']['existing_entries']

def _get_canonical_map(canonical_id: str, dbh: Database, id_collection: str = SECOND_DEFAULT) -> Union[dict, None]:
    ''' Gets the canonical ID data record.

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
    '''
    return dbh[id_collection].find_one({'biomarker_canonical_id': canonical_id})

def _get_key(document: dict) -> str:
    ''' Gets the key to compare against for second level ID generation.

    Parameters
    ----------
    document: dict
        The document to extract the key from.

    Returns
    -------
    str
        Either the condition ID or the exposure agent ID
    '''
    key1 = 'condition'
    key2 = 'exposure_agent'
    if key1 in document.keys() and document[key1] is not None and document[key1]['condition_id']:
        return document[key1]['condition_id']
    elif key2 in document.keys() and document[key2] is not None and document[key2]['exposure_agent_id']:
        return document[key2]['exposure_agent_id']
    else:
        logging.error(f'Error when parsing document for second level ID key.\nDocument: {document}')
        sys.exit(1)
