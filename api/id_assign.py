import logging 
import os 
import argparse 
import sys
import glob 
import pymongo
import requests 
from pymongo.errors import DuplicateKeyError
from datetime import datetime
from id import *
import misc_functions as misc_fns
import deepdiff as dd

# batch size for mongodb bulk write operations
BATCH_SIZE = 1000
# threshold for updating the local storage replica of the id_map_collection
UPDATE_THRESHOLD = 800

def process_data(data: list, dbh, id_collection: str, filepath: str) -> tuple:
    ''' Processes the data for ID assignments. 

    Parameters
    ----------
    data: dict or list
        The data to process.
    dbh: pymongo.MongoClient
        The database handle.
    id_collection: str
        The name of the collection to check for hash collisions.
    id_collection_list: list
        The local storage replicate of the id_map_collection.
    filepath: str
        The filepath to the data file.
    
    Returns
    -------
    tuple
        The updated data with the new biomarker ids and the new entries for the local storage replica of the id_map_collection. 
    '''
    if not data:
        logging.error(f'No data found for {filepath}.')
        print(f'No data found for {filepath}.')

    return_data = []
    collision_count = 0
    collisions = {}
    collision_report_filename = f'{os.path.splitext(os.path.split(filepath)[1])[0]}_collisions.json'
    collision_report_path = f'./collision_reports/{collision_report_filename}'
    new_id_entries = []

    for idx, document in enumerate(data):
        biomarker_id, hash_value, collision, core_values_str = id_assign(document, dbh, id_collection)
        document['biomarker_id'] = biomarker_id
        # if there is a hash collision, handle appropriately 
        if collision:
            existing_entry = get_record_by_id(biomarker_id, dbh, id_collection)
            if existing_entry:
                difference = dd.diff(existing_entry, document, ignore_order = True)
            else:
                difference = f'Existing entry not found for biomarker id {biomarker_id}.'
            collisions[collision_count + 1] = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'biomarker_id': biomarker_id,
                'hash_value': hash_value,
                'file': filepath,
                'core_values_str': core_values_str,
                'difference': difference
            }
            collision_count += 1
            output_message = f'\nCollision detected for record number {idx} on biomarker id {biomarker_id}:\n\tFile: {filepath}\n\tCore Values Str: {core_values_str}\n\tHash Value: {hash_value}\n'
            logging.warning(output_message)
            print(output_message)
            document['collision'] = 1
        # if no hash collision, add the hash value to the database and add the biomarker id to the document
        else:
            new_id_entries.append({
                "hash_value": hash_value,
                "ordinal_id": biomarker_id,
                "core_values_str": core_values_str
            })
            document['collision'] = 0
        return_data.append(document)
        
    misc_fns.write_json(collision_report_path, collisions)
    logging.info(f'Finished assigning ID\'s for {filepath}, Collision count: {collision_count}.')
    return return_data, new_id_entries

def update_local_id_collection(id_collection_local_path: str, new_id_data: list) -> None:
    ''' Updates the local storage replica of the id_map_collection.

    Parameters
    ----------
    id_collection_local_path: str
        The filepath to the local storage replica of the id_map_collection.
    new_id_data: list
        The new entries to add to the local storage replica of the id_map_collection.
    '''
    existing_id_data = misc_fns.load_json(id_collection_local_path)
    existing_id_data.extend(new_id_data)
    misc_fns.write_json(id_collection_local_path, existing_id_data)

def main():

    ### handle command line arguments
    parser = argparse.ArgumentParser(
        prog = 'id_assign.py',
        usage = 'python id_assign.py [options] server',
    )
    parser.add_argument('-s', '--server', help = 'tst')
    options = parser.parse_args()
    if not options.server:
        parser.print_help()
        sys.exit(1)
    server = options.server
    if server.lower() == 'prd':
        print('Cannot run this script on prd server.')
        sys.exit(1)
    if server.lower() not in {'tst'}:
        print('Invalid server name.')
        sys.exit(1)

    ### get config info for database connection
    config_obj = misc_fns.load_json('config.json')
    mongo_port = config_obj['dbinfo']['port'][server]
    host = f'mongodb://127.0.0.1:{mongo_port}'
    db_name = config_obj['dbinfo']['dbname']
    db_user = config_obj['dbinfo'][db_name]['user']
    db_pass = config_obj['dbinfo'][db_name]['password']
    data_root_path = config_obj['data_path']
    id_collection = config_obj['dbinfo'][db_name]['id_collection']
    # get the database handle
    dbh = misc_fns.get_mongo_handle(host, db_name, db_user, db_pass)

    ### setup logger
    misc_fns.setup_logging(f'./logs/id_assign.log')
    logging.info(f'Beginning ID assignment process #####################')

    ### setup first run hash_value index 
    misc_fns.setup_index(dbh, 'hash_value', id_collection, 'hash_value_1')

    ### initiate id assignment logic 
    data_release_glob_pattern = f'{data_root_path}/generated/datamodel/new_data/current/*.json'
    id_collection_local_path = f'{data_root_path}/generated/datamodel/id_collection.json'
    new_id_collection = []
    for fp in glob.glob(data_release_glob_pattern):
        data = misc_fns.load_json(fp)
        updated_data, new_id_entries = process_data(data, dbh, id_collection, fp)
        misc_fns.write_json(fp, updated_data)
        # append new id entries to local storage replica of id_map_collection
        new_id_collection.extend(new_id_entries)
        if len(new_id_collection) >= UPDATE_THRESHOLD:
            update_local_id_collection(id_collection_local_path, new_id_collection)
    
    # write any leftover new id entries to local storage replica of id_map_collection
    if new_id_collection:
        update_local_id_collection(id_collection_local_path, new_id_collection)
        
    logging.info(f'Finished ID assignment process ---------------------')
        
if __name__ == '__main__':
    main()