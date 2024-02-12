''' Handles the ID assignment/collision operations. 
'''

import sys
import hashlib
import re
import pymongo
import logging 
import misc_functions as misc_fns

def id_assign(document: dict, dbh, id_collection: str = 'id_map_collection') -> tuple:
    ''' Goes through the ID assign process for the passed document.

    Parameters
    ----------
    document: dict
        The document to assign the ID for.
    id_collection: str (default: 'id_map_collection')
        The name of the collection to check for hash collisions.
    
    Returns
    -------
    tuple: (str, str , bool, str)
        The assigned biomarker ID, hash value, a collision boolean, and the core values string.
    '''
    # generate the hash value and core values string
    hash_value, core_values_str = _generate_custom_id(document)

    # check if the hash value already exists in the database
    collision_status = _check_collision(hash_value, dbh)
    if collision_status:
        ordinal_id  = _get_ordinal_id(hash_value, dbh)
    else:
        # add the hash value and incremented ordinal ID to the id collection
        ordinal_id = _add_hash_and_increment_ordinal(hash_value, core_values_str, dbh, id_collection)
    
    return ordinal_id, hash_value, collision_status, core_values_str

def get_record_by_id(biomarker_id: str, dbh, db_collection: str) -> dict:
    ''' Gets the record by the biomarker ID.

    Parameters
    ----------
    biomarker_id: str
        The biomarker ID to get the record for.
    dbh: pymongo.MongoClient
        The database handle.
    db_collection: str
        The name of the collection to get the record from.
    
    Returns
    -------
    dict
        The record for the biomarker ID.
    '''
    return dbh[db_collection].find_one({'biomarker_id': biomarker_id}, {'_id': 0})

def _generate_custom_id(document: dict) -> tuple: 
    ''' Generates the custom hash ID for the document.

    Parameters
    ----------
    document: json
        The document to generate the ID for.
    
    Returns
    -------
    tuple: (str, str)
        Returns the custom hash ID and the concatenated core values string.
    '''
    # hold the core field values 
    core_values = []
    
    # grab the core fields from the biomarker component
    for component in document['biomarker_component']:
        core_values.append(_extract_change(component['biomarker']))
        core_values.append(component['assessed_biomarker_entity_id'])
    
    # grab top level core fields 
    if 'condition' in document.keys() and document['condition'] != None:
        core_values.append(document['condition']['condition_id'])
    elif 'exposure_agent' in document.keys() and document['exposure_agent'] != None:
        core_values.append(document['exposure_agent']['exposure_agent_id'])

    # clean the core values 
    core_values = [misc_fns.clean_value(v) for v in core_values] 
    # inplace sort the core_values alphabetically
    core_values.sort()
    core_values_str = '_'.join(core_values)

    # generate the SHA-256 hash of the core values
    return hashlib.sha256(core_values_str.encode('utf-8')).hexdigest(), core_values_str

def _extract_change(biomarker: str) -> str:
    ''' Extracts the change from the biomarker string. For now, naive implementation (grabs first word).

    Parameters
    ----------
    biomarker: str
        The biomarker string to extract the change from.
    
    Returns
    -------
    str
        The extracted change.
    '''
    return biomarker.split(' ')[0]

def _check_collision(hash_value: str, dbh, id_collection: str = 'id_map_collection') -> bool:
    ''' Checks if the hash value already exists in the database. 

    Parameters
    ----------
    hash_value: str
        The hash value to check.
    dbh: pymongo.MongoClient
        The database handle.
    id_collection: str
        The name of the collection to check for the hash value.
    
    Returns
    -------
    bool
        True if the hash value already exists in the database, False otherwise.
    '''
    # check if the hash value already exists in the database
    if dbh[id_collection].find_one({'hash_value': hash_value}) != None:
        return True
    return False

def _increment_ordinal_id(ordinal_id: str) -> str:
    ''' Increments the ordinal id.

    Parameters
    ----------
    ordinal_id: str
        The current max ordinal ID to increment.
    
    Returns
    -------
    str
        The incremented ordinal ID.
    '''
    # extract the letters and numbers from the ordinal ID
    letters = ordinal_id[:2]
    numbers = int(ordinal_id[2:])

    # increment the numbers
    if numbers < 9999:
        return letters + str(numbers + 1).zfill(4)
    # check if the maximum ordinal ID has been reached
    if letters == 'ZZ':
        raise ValueError('Maximum ordinal ID reached.')
    
    # increment the letters
    first_letter, second_letter = letters
    # roll over the second letter
    if second_letter == 'Z':
        first_letter = chr(ord(first_letter) + 1)
        second_letter = 'A'
    else:
        second_letter = chr(ord(second_letter) + 1)
    
    return first_letter + second_letter + '0000'

def _get_ordinal_id(hash_value: str, dbh, id_collection: str = 'id_map_collection') -> str:
    ''' Gets the ordinal ID for the hash value.

    Parameters
    ----------
    hash_value: str
        The hash value to get the ordinal ID for.
    dbh: pymongo.MongoClient
        The database handle.
    id_collection: str (default: 'id_map_collection')
        The name of the collection to get the ordinal ID from.
    
    Returns
    -------
    str
        The ordinal ID for the hash value.
    '''
    return dbh[id_collection].find_one({'hash_value': hash_value})['ordinal_id']

def _add_hash_and_increment_ordinal(hash_value: str, core_values_str: str, dbh, id_collection: str = 'id_map_collection') -> str:
    ''' For a new unique entry, the hash value and core values string to the id collection and assigns an 
    incremented ordinal ID.

    Parameters
    ----------
    hash_value: str
        The hash value to add.
    core_values_str: str
        The core values string to add.
    dbh: pymongo.MongoClient
        The database handle.
    id_collection: str (default: 'id_map_collection')
        The name of the collection to add the hash value and core values string to.
    
    Returns
    -------
    str
        The newly assigned ordinal ID to be used as the human readable biomarker ID.
    '''
    # grab the current max ordinal ID
    max_entry = dbh[id_collection].find_one(sort=[('ordinal_id', pymongo.DESCENDING)])
    max_ordinal_id = max_entry['ordinal_id'] if max_entry else 'AA0000'

    # increment the ordinal ID
    try:
        new_ordinal_id = _increment_ordinal_id(max_ordinal_id)
    except ValueError as e:
        print(f'ValueError: {e}')
        logging.error(e)
        sys.exit(1)

    # add the hash value, incremented ordinal id, and core values string to the id collection
    dbh[id_collection].insert_one({'hash_value': hash_value, 'ordinal_id': new_ordinal_id, 'core_values_str': core_values_str})

    return new_ordinal_id

def validate_id_format(biomarker_id: str) -> bool:
    ''' Validates the format of the biomarker ID for entering into prd database.

    Parameters
    ----------
    biomarker_id: str
        The biomarker ID to validate.
    
    Returns
    -------
    bool
        True if the biomarker ID is valid, False otherwise.
    '''
    # validate that the biomarker id is formatted as XX####, where X is a letter and # is a number
    if re.match(r'[A-Z]{2}\d{4}', biomarker_id) == None:
        return False
    return True


