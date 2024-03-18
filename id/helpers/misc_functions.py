''' Miscellaneous functions and MongoDB functions used throughout the ID assignment process. 

Public functions:
    load_json: Loads data from a JSON file.
    write_json: Writes data to a JSON file.
    get_mongo_handle: Sets up a MongoDB connection and returns a database handle.
    setup_index: Sets up a unique index on a specified field. 
    setup_logging: Sets up a logger.
    validate_filepath: Validates a filepath. 
    copy_file: Copies files from a source to destination.
    clean_value: Removes all non-alphanumeric chracters and lowercases a string.
    get_user_confirmation: Gets user confirmation or deial for an operation.
'''

import logging
import sys
import os
import re 
import json
import pymongo
import pymongo.errors
from pymongo.database import Database
import subprocess
from typing import Union

def load_json(filepath: str) -> Union[dict, list]:
    ''' Loads a JSON file.

    Parameters
    ----------
    filepath: str
        The path to the JSON file.

    Returns
    -------
    dict or list
        The JSON object.
    '''
    try:
        with open(filepath, 'r') as f:
            json_obj = json.load(f)
    except FileNotFoundError as e:
        print(f"FileNotFoundError in load_json for filepath: `{filepath}`.\n{e}") 
        sys.exit(1)
    return json_obj

def write_json(filepath: str, data: list) -> None:
    ''' Writes a JSON file.

    Parameters
    ----------
    filepath: str
        The path to the JSON file.
    data: dict or list
        The data to write to the JSON file.
    '''
    with open(filepath, 'w') as f:
        json.dump(data, f, indent = 4)

def get_mongo_handle(host: str, authSource: str, username: str, password: str, db_name: str = '', authMechanism: str = 'SCRAM-SHA-1', serverSelectionTimeoutMS: int = 10000) -> Union[Database, None]:
    ''' Gets a MongoDB handle.

    Parameters
    ----------
    host: str
        The MongoDB host (including port).
    authSource: str
        The MongoDB authentication source.
    username: str
        The MongoDB username.
    password: str
        The MongoDB password.
    db_name: str (default = authSource)
        The MongoDB database name.
    authMechanism: str (default = 'SCRAM-SHA-1')
        The MongoDB authentication mechanism.
    serverSelectionTimeoutMS: int (default = 10000)
        The MongoDB server selection timeout in milliseconds.
    '''
    if not db_name:
        db_name = authSource
    try:
        client: pymongo.MongoClient = pymongo.MongoClient(host,
                                    username = username,
                                    password = password,
                                    authSource = authSource,
                                    authMechanism = authMechanism,
                                    serverSelectionTimeoutMS = serverSelectionTimeoutMS)
        # test the connection
        client.server_info()
    except pymongo.errors.ServerSelectionTimeoutError as e:
        print(e)
        return None
    except Exception as e:
        print(e)
        return None
    
    return client[db_name]

def setup_index(dbh, index_col: str, collection_name: str, index_name: str = '') -> None:
    ''' Sets up an index on the specified index_name in the specified collection.

    Parameters
    ----------
    dbh: pymongo.MongoClient
        The database handle.
    index_col: str
        The field to index.
    collection_name: str 
        The name of the collection to create the index in.
    index_name: str (default = f'{index_col}_1')
        The name of the index to create.
    '''
    if not index_name:
        index_name = f'{index_col}_1'
    if index_name not in dbh[collection_name].index_information():
        dbh[collection_name].create_index([(index_col, pymongo.ASCENDING)], name = index_name, unique = True)
        logging.info(f'Created index {index_name} on {index_col} in {collection_name} collection.')
    else:
        logging.info(f'Index {index_name} on {index_col} in {collection_name} collection already exists.')

def setup_logging(log_path: str) -> None:
    ''' Sets up logging.

    Parameters
    ----------
    log_path: str
        The path to the log file.
    '''
    logging.basicConfig(filename = log_path, level = logging.DEBUG,
                        format = '%(asctime)s %(levelname)s %(message)s')

def validate_filepath(filepath: str, mode: str) -> bool:
    ''' Validates the filepaths for the user inputted source path and
    the destination path. 

    Parameters
    ----------
    filepath: str
        Filepath to the source data dictionary file or the output path.
    mode: str
        Whether checking the output directory path or the input file. ('input' or 'output')
    
    Returns
    -------
    bool
        True if the filepath is valid, False otherwise.
    '''
    
    if mode == 'input':
        if not os.path.isfile(filepath):
            print(f'Error: The (input) file {filepath} does not exist.')
            return False 
    elif mode == 'output':
        if not os.path.isdir(filepath):
            print(f'Error: The (output) directory {filepath} does not exist.')
            return False
    else:
        print(f'Error: Invalid mode {mode}.')
        return False
    return True

def copy_file(src: str, dest: str) -> bool:
    ''' Copies a file from src to dest.

    Parameters
    ----------
    src: str
        The source filepath.
    dest: str
        The destination filepath.
    
    Returns
    -------
    bool
        True if the file was copied successfully, False otherwise.
    '''
    if not validate_filepath(src, 'input') or not validate_filepath(dest, 'output'):
        return False
    dest_file = os.path.join(dest, os.path.basename(src))
    if os.path.exists(dest_file):
        print(f'Error: File {dest_file} already exists.')
        return False
    try:
        subprocess.run(['cp', src, dest], check = True)
        return True
    except subprocess.CalledProcessError as e:
        print(e)
        return False 

def clean_value(value: str) -> str:
    ''' Cleans the passed value using regex. Removes all non-alphanumeric 
    characters and makes the value lowercase.

    Parameters
    ----------
    value: str
        The value to clean.
    
    Returns
    -------
    str
        The cleaned value.
    '''
    value = re.sub(r'[^a-zA-Z0-9]', '', value).lower()
    return value 

def get_user_confirmation() -> bool:
    ''' Prompts the user for a confirmation or denial.

    Returns
    ----------
    bool
        Whether the user confirmed or denied.
    '''
    while True:
        user_input = input('Continue? (y/n)').strip().lower()
        if user_input == 'y':
            return True
        elif user_input == 'n':
            return False
        else:
            print("Please enter 'y' for yes or 'n' for no.")
