''' Handles the ID assignment/collision operations. 

Public functions:
    id_assign: Goes through the ID assign process for the passed document. 
    get_record_by_id: Gets the record by the biomarker ID.

Private functions:

'''

import sys
import hashlib
import re
import pymongo
from pymongo.database import Database
import logging 
import misc_functions as misc_fns
from typing import Union
import canonical_helpers as canonical
import second_level_helpers as second

CANONICAL_DEFAULT = canonical.CANONICAL_DEFAULT
SECOND_DEFAULT = second.SECOND_DEFAULT

def id_assign(document: dict, dbh: Database, canonical_id_collection: str = CANONICAL_DEFAULT, second_id_collection: str = SECOND_DEFAULT) -> tuple:
    ''' Goes through the ID assign process for the passed document.

    Parameters
    ----------
    document: dict
        The document to assign the ID for.
    dbh: Database
        The database handle.
    id_collection: str (default: 'id_map_collection')
        The name of the collection to check for hash collisions.
    
    Returns
    -------
    tuple: (str, str, str , bool, str)
        The assigned canonical biomarker ID, second level ID, hash value, a collision boolean, and the core values string.
    '''
    canonical_id, hash_value, core_values_str, canonical_collision = canonical.get_ordinal_id(document, dbh, CANONICAL_DEFAULT)
    second_level_id, second_level_collision = second.get_second_level_id(
        canonical_id,
        canonical_collision,
        document,
        dbh,
        SECOND_DEFAULT
    )

def get_record_by_id(biomarker_id: str, canonical_level: str, dbh: Database, db_collection: str) -> Union[dict, None]:
    ''' Gets the record by the biomarker ID at the specified level.

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
    '''
    search_field = 'biomarker_canonical_id' if canonical_level else 'biomarker_id'
    record = dbh[db_collection].find_one({search_field: biomarker_id}, {'_id': 0})
    return record
