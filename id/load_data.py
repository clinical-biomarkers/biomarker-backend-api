''' Script that handles the data load into the MongoDB instance.
'''

import subprocess
import sys
import os
import glob
import pymongo 
from pymongo.database import Database
from helpers import misc_functions as misc_fns
import argparse
import logging 
from helpers import id_backend as id_backend

BATCH_SIZE = 1000

def preprocess_checks(data: list) -> bool:
    ''' Performs preprocessing checks on the data before .

    Parameters
    ----------
    data: dict or list
        The data to check.
        
    Returns
    -------
    bool
        True if all checks pass, False otherwise.
    '''
    for document in data:
        canonical_validation = id_backend.validate_id_format(document['biomarker_canonical_id'], 0)
        second_level_validation = id_backend.validate_id_format(document['biomarker_id'], 1)
        collisision_key_check = 'collision' in document
        preprocess_conditions = (canonical_validation and second_level_validation and collisision_key_check)
        if not preprocess_conditions:
            return False
    return True

def process_data(data: list, dbh: Database, db_collection: str, collision_collection: str, fp: str, collision_full: bool) -> bool:
    ''' Inserts the data into the prd database.

    Parameters
    ----------
    data: dict or list
        The data to process.
    dbh: Database
        The database handle.
    db_collection: str
        The name of the collection to insert the data into.
    collision_collection: str
        The name of the collection to insert the unreviewed data into.
    fp: str
        The filepath to the data file.
    collision_full: bool
        Whether to entirely load into the unreviewed collection.
    
    Returns
    -------
    bool
        True if data was loaded, False otherwise.
    '''
    if not preprocess_checks(data):
        logging.error(f'Preprocessing checks failed for file: \'{fp}\'.')
        print(f'Preprocessing checks failed for file: \'{fp}\'.')
        return False

    bulk_ops = []
    collision_ops = []

    for idx, document in enumerate(data):

        collision_status = document.pop('collision')
        if collision_status == 2:
            logging.info(
                f"Skipping index `{idx}` in file `{os.path.basename(fp)}` for hard collision\
                \n\tbiomarker canonical ID: `{document['biomarker_canonical_id']}`\
                \n\tbiomarker ID: `{document['biomarker_id']}`"
            )
            continue
        elif collision_status == 0:
            if collision_full:
                collision_ops.append(pymongo.InsertOne(document))
            else:
                bulk_ops.append(pymongo.InsertOne(document))
        elif collision_status == 1:
            collision_ops.append(pymongo.InsertOne(document))

        if len(bulk_ops) >= BATCH_SIZE:
            try:
                dbh[db_collection].bulk_write(bulk_ops, ordered = False)
                bulk_ops = []
            except Exception as e:
                print(f'\nError during bulk ops write:\n\tFile: {fp}\n\tError: {e}.')
                logging.error(f'\nError during bulk ops write:\n\tFile: {fp}\n\tError: {e}.')
                return False 
        if len(collision_ops) >= BATCH_SIZE:
            try:
                dbh[collision_collection].bulk_write(collision_ops, ordered = False)
                collision_ops = []
            except Exception as e:
                print(f'\nError during collision ops write:\n\tFile: {fp}\n\tError: {e}.')
                logging.error(f'\nError during collision ops write:\n\tFile: {fp}\n\tError: {e}.')
                return False
    
    if bulk_ops:
        try:
            dbh[db_collection].bulk_write(bulk_ops, ordered = False)
        except Exception as e:
            print(f'\nError during bulk ops write:\n\tFile: {fp}\n\tError: {e}.')
            logging.error(f'\nError during bulk ops write:\n\tFile: {fp}\n\tError: {e}.')
            return False 
    if collision_ops:
        try:
            dbh[collision_collection].bulk_write(collision_ops, ordered = False)
        except Exception as e:
            print(f'\nError during collision ops write:\n\tFile: {fp}\n\tError: {e}.')
            logging.error(f'\nError during collision ops write:\n\tFile: {fp}\n\tError: {e}.')
            return False
    
    return True 

def load_id_collection(connection_string: str, load_path: str, collection: str) -> bool:
    ''' Loads the local ID collections into the prod database.

    Parameters
    ----------
    connection_string: str
        Connection string for the MongoDB connection.
    load_path: str
        The filepath to the local ID map. 
    collection: str
        The collection to load into.

    Returns
    -------
    bool
        Indication if the collection was loaded successfully.
    '''
    command = [
        'mongoimport',
        '--uri', connection_string,
        '--collection', collection,
        '--file', load_path,
        '--mode', 'upsert'
    ]

    try:
        subprocess.run(command, check = True)
    except subprocess.CalledProcessError as e:
        print('Args passed:')
        print(f'Connection string: {connection_string}')
        print(f'Load path: {load_path}')
        print(f'Collection: {collection}')
        print(e)
        return False
    return True
    
def main(): 
    
    ### handle command line arguments
    parser = argparse.ArgumentParser(
        prog = 'load_data.py',
        usage = 'python load_data.py [options] server'
    )
    parser.add_argument('-s', '--server', help = 'tst/prd')
    options = parser.parse_args()
    if not options.server:
        parser.print_help()
        sys.exit(1)
    server = options.server
    if server.lower() not in {'tst', 'prd'}:
        print('Invalid server name.')
        sys.exit(1)

    ### get config info
    config_obj = misc_fns.load_json('../api/config.json')
    if not isinstance(config_obj, dict):
        print(f"Error reading config JSON, expected type `dict`, got {type(config_obj)}.")
        sys.exit(1)
    mongo_port = config_obj['dbinfo']['port'][server]
    host = f'mongodb://127.0.0.1:{mongo_port}'
    db_name = config_obj['dbinfo']['dbname']
    db_user = config_obj['dbinfo'][db_name]['user']
    db_pass = config_obj['dbinfo'][db_name]['password']
    data_root_path = config_obj['data_path']
    canonical_id_collection = config_obj['dbinfo'][db_name]['canonical_id_map']
    second_level_id_collection = config_obj['dbinfo'][db_name]['second_level_id_map']
    data_collection = config_obj['dbinfo'][db_name]['collection']
    unreviewed_collection = config_obj['dbinfo'][db_name]['unreviewed_collection']
    dbh = misc_fns.get_mongo_handle(host, db_name, db_user, db_pass)

    ### setup logger
    misc_fns.setup_logging(f'./logs/load_data_{server}.log')
    logging.info(f'Loading data for server: {server}. #####################')

    ### setup first run biomarker_id index 
    misc_fns.setup_index(dbh, 'biomarker_canonical_id', data_collection, 'biomarker_canonical_id_1')

    ### load the load map
    load_map = misc_fns.load_json(f'{data_root_path}/generated/datamodel/new_data/current/load_map.json')
    if not isinstance(load_map, dict):
        print(f"Error reading load map JSON, expected type `dict`, got {type(load_map)}.")
        sys.exit(1)
    
    ### begin processing data 
    data_release_glob_pattern = f'{data_root_path}/generated/datamodel/new_data/current/*.json'
    total_files = glob.glob(data_release_glob_pattern)
    _, unreviewed_files = misc_fns.load_map_confirmation(load_map, total_files)

    # if running on prd server, load the id_collection.json file
    if server == 'prd':
        canonical_id_collection_local_path = f'{data_root_path}/generated/datamodel/canonical_id_collection.json'
        second_level_id_collection_local_path = f'{data_root_path}/generated/datamodel/second_level_id_collection.json'
        connection_string = misc_fns.create_connection_string(host, db_user, db_pass, db_name)
        if load_id_collection(connection_string, canonical_id_collection_local_path, canonical_id_collection):
            print('Successfully loaded canonical ID map into prod database.')
        else:
            print('Failed loading canonical ID map into prod database. You will have to update manually.')
        if load_id_collection(connection_string, second_level_id_collection_local_path, second_level_id_collection):
            print('Successfully loaded secondary ID map into prod database.')
        else:
            print('Failed loading secondary ID map into prod database. You will have to update manually.')

    for fp in glob.glob(data_release_glob_pattern):
        if fp == f'{data_root_path}/generated/datamodel/new_data/current/load_map.json':
            continue
        data = misc_fns.load_json(fp)
        if not isinstance(data, list):
            print(f'Error reading data file `{fp}`, expected list, got `{type(data)}`. Skipping...')
            continue

        if os.path.basename(fp) in unreviewed_files:
            collision_full = True
        else:
            collision_full = False
        
        if process_data(data, dbh, data_collection, unreviewed_collection, fp, collision_full):
            if collision_full:
                output = f'Successfully loaded data into unreviewed collection for file: {fp}.'
            else:
                output = f'Successfully loaded data into reviewed collection for file: {fp}.'
        else:
            if collision_full:
                output = f'Failed to load data entirely into unreviewed collection for file: {fp}.'
            else:
                output = f'Failed to load data entirely into reviewed collection for file: {fp}.'
        logging.error(output)
        print(output)

    logging.info(f'Finished loading data for server: {server}. ---------------------')

if __name__ == '__main__': 
    main() 

