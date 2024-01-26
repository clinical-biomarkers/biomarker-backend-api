import sys
import glob
import pymongo 
from pymongo.errors import DuplicateKeyError
import misc_functions as misc_fns
from id import *
import argparse
import logging 

BATCH_SIZE = 1000

def preprocess_checks(data: dict | list, dbh, db_collection: str) -> bool:
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
        # check for duplicate biomarker ids
        if dbh[db_collection].find_one({'biomarker_id': document['biomarker_id']}):
            return False
    return True

def process_data(data: dict | list, dbh, db_collection: str, fp: str) -> bool:
    ''' Inserts the data into the prd database.

    Parameters
    ----------
    data: dict or list
        The data to process.
    dbh: pymongo.MongoClient
        The database handle.
    db_collection: str
        The name of the collection to insert the data into.
    fp: str
        The filepath to the data file.
    
    Returns
    -------
    bool
        True if data was loaded, False otherwise.
    '''
    if not preprocess_checks(data, dbh, db_collection):
        logging.error(f'Preprocessing checks failed for file: \'{fp}\'. Collisions should be handled before attempting to load the data.')
        print(f'Preprocessing checks failed for file: \'{fp}\'. Collisions should be handled before attempting to load the data.')
        return False

    bulk_ops = []

    for document in data:
        bulk_ops.append(pymongo.InsertOne(document))
        if len(bulk_ops) >= BATCH_SIZE:
            try:
                dbh[db_collection].bulk_write(bulk_ops)
                bulk_ops = []
            except DuplicateKeyError as e:
                print(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
                logging.error(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
                return False
            except Exception as e:
                print(f'\nError:\n\tFile: {fp}\n\tError: {e}.')
                logging.error(f'\nError:\n\tFile: {fp}\n\tError: {e}.')
                return False 
    
    if bulk_ops:
        try:
            dbh[db_collection].bulk_write(bulk_ops)
        except DuplicateKeyError as e:
            print(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
            logging.error(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
            return False 
        except Exception as e:
            print(f'\nError:\n\tFile: {fp}\n\tError: {e}.')
            logging.error(f'\nError:\n\tFile: {fp}\n\tError: {e}.')
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
        usage = 'python load_data.py [options] server ver'
    )
    parser.add_argument('-s', '--server', help = 'tst/prd')
    options = parser.parse_args()
    if not options.server or not options.ver:
        parser.print_help()
        sys.exit(1)
    server = options.server

    ### get config info
    config_obj = misc_fns.load_json('config.json')
    mongo_port = config_obj['dbinfo']['port'][server]
    host = f'mongodb://127.0.0.1:{mongo_port}'
    db_name = config_obj['dbinfo']['dbname']
    data_root_path = config_obj['data_path']
    db_collection = config_obj['dbinfo'][db_name]['collection']
    id_collection = config_obj['dbinfo'][db_name]['id_collection']
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
        if process_data(data, dbh, db_collection, fp):
            logging.info(f'Successfully loaded data for file: {fp}.')
            print(f'Successfully loaded data for file: {fp}.')
        else:
            logging.error(f'Failed to load data for file: {fp}.')
            print(f'Failed to load data for file: {fp}.')

    logging.info(f'Finished loading data for server: {server}. ---------------------')

if __name__ == '__main__': 
    main() 

