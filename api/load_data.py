import sys
import glob
import pymongo 
from pymongo.errors import DuplicateKeyError
import misc_functions as misc_fns
from id import *
import argparse
import logging 

BATCH_SIZE = 1000

def preprocess_checks(data: list) -> bool:
    ''' Performs preprocessing checks on the data before .

    Parameters
    ----------
    data: dict or list
        The data to check.
    dbh: pymongo.MongoClient
        The database handle.
    db_collection: str
        The name of the collection to check for duplicate collisions.
    
    Returns
    -------
    bool
        True if all checks pass, False otherwise.
    '''
    for document in data:
        # check that the document has a valid formatted biomarker id
        if not validate_id_format(document['biomarker_id']):
            return False
    return True

def process_data(data: list, dbh, db_collection: str, collision_collection: str, fp: str) -> bool:
    ''' Inserts the data into the prd database.

    Parameters
    ----------
    data: dict or list
        The data to process.
    dbh: pymongo.MongoClient
        The database handle.
    db_collection: str
        The name of the collection to insert the data into.
    collision_collection: str
        The name of the collection to insert the collision data into.
    fp: str
        The filepath to the data file.
    
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
        if 'collision' not in document: 
            print(f'No collision key found for entry {idx} in file {fp}.')
            logging.error(f'No collision key found for entry {idx} in file {fp}.')
        collision_status = document.pop('collision')
        if collision_status == 0:
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

def load_id_collection(id_collection_data: list, dbh, id_collection: str) -> None:
    ''' Loads the id_collection.json file into the prod database.

    Parameters
    ----------
    id_collection_data: list
        The data to load into the id_collection.
    dbh: pymongo.MongoClient
        The database handle.
    id_collection: str
        The name of the collection to insert the data into.
    '''
    if not id_collection_data:
        logging.error(f'No data found for id_collection_data.')
        print(f'No data found for id_collection_data.')
        return
    misc_fns.setup_index(dbh, 'hash_value', id_collection, 'hash_value_1') 
    bulk_ops = [pymongo.InsertOne(document) for document in id_collection_data]
    try:
        if bulk_ops:
            dbh[id_collection].bulk_write(bulk_ops, ordered = False)
            logging.info(f'Successfully loaded id_collection_data.')
            print(f'Successfully loaded id_collection_data.')
    except DuplicateKeyError as e:
        pass
    except Exception as e:
        logging.error(f'Error loading id_collection_data into prod database: {e}.') 
        print(f'Error loading id_collection_data into prod database: {e}.')

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
    config_obj = misc_fns.load_json('config.json')
    mongo_port = config_obj['dbinfo']['port'][server]
    host = f'mongodb://127.0.0.1:{mongo_port}'
    db_name = config_obj['dbinfo']['dbname']
    data_root_path = config_obj['data_path']
    db_collection = config_obj['dbinfo'][db_name]['collection']
    id_collection = config_obj['dbinfo'][db_name]['id_collection']
    collision_collection = config_obj['dbinfo'][db_name]['collision_collection']
    db_user = config_obj['dbinfo'][db_name]['user']
    db_pass = config_obj['dbinfo'][db_name]['password']
    # get the database handle
    dbh = misc_fns.get_mongo_handle(host, db_name, db_user, db_pass)

    ### setup logger
    misc_fns.setup_logging(f'./logs/load_data_{server}.log')
    logging.info(f'Loading data for server: {server}. #####################')

    ### setup first run biomarker_id index 
    misc_fns.setup_index(dbh, 'biomarker_id', db_collection, 'biomarker_id_1')
    
    ### begin processing data 
    data_release_glob_pattern = f'{data_root_path}/generated/datamodel/new_data/current/*.json'
    # if running on prd server, load the id_collection.json file
    if server == 'prd':
        id_collection_json_path = f'{data_root_path}/generated/datamodel/id_collection.json'
        load_id_collection(misc_fns.load_json(id_collection_json_path), dbh, id_collection)

    for fp in glob.glob(data_release_glob_pattern):
        data = misc_fns.load_json(fp)
        if process_data(data, dbh, db_collection, collision_collection, fp):
            logging.info(f'Successfully loaded data for file: {fp}.')
            print(f'Successfully loaded data for file: {fp}.')
        else:
            logging.error(f'Failed to load data for file: {fp}.')
            print(f'Failed to load data for file: {fp}.')

    logging.info(f'Finished loading data for server: {server}. ---------------------')

if __name__ == '__main__': 
    main() 

