import logging 
import os 
import argparse 
import sys
import glob 
import pymongo
from pymongo.errors import DuplicateKeyError
from datetime import datetime
from id import *
import misc_functions as misc_fns

BATCH_SIZE = 1000

def process_data(data: dict | list, dbh, id_collection: str, filepath: str) -> list:
    ''' Processes the data for ID assignments. 

    Parameters
    ----------
    data: dict
        The data to process.
    dbh: pymongo.MongoClient
        The database handle.
    id_collection: str
        The name of the collection to check for hash collisions.
    filepath: str
        The filepath to the data file.
    
    Returns
    -------
    list
        The updated data with the new biomarker ids. 
    '''
    return_data = []
    collision_count = 0
    collisions = {}
    collision_report_filename = f'{os.path.splitext(os.path.split(filepath)[1])[0]}_collisions.json'
    collision_report_path = f'./collision_reports/{collision_report_filename}'

    for document in data:
        # generate hash value for data record
        hash_value, core_values_str = generate_custom_id(document)
        # if there is a hash collision, handle appropriately 
        if check_collision(hash_value, dbh, id_collection):
            collisions[collision_count + 1] = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'file': filepath,
                'core_values_str': core_values_str,
                'hash_value': hash_value,
                'document': document
            }
            collision_count += 1
            output_message = f'\nCollision detected for record in:\n\tFile: {filepath}:\n\tDocument: {document}\n\tCore Values Str: {core_values_str}\n\tHash Value: {hash_value}\n'
            logging.warning(output_message)
            print(output_message)
            biomarker_id_collision_val = f"COLLISION_{document['biomarker_id']}"
            document['biomarker_id'] = biomarker_id_collision_val
            return_data.append(document)
        # if no hash collision, add the hash value to the database and add the biomarker id to the document
        else:
            biomarker_id = add_hash_and_increment_ordinal(hash_value, core_values_str, dbh, id_collection)
            document['biomarker_id'] = biomarker_id
            return_data.append(document)
        
    misc_fns.write_json(collision_report_path, collisions)
    logging.info(f'Finished assigning ID\'s for {filepath}, Collision count: {collision_count}.')
    return return_data

def main():

    ### handle command line arguments
    parser = argparse.ArgumentParser(
        prog = 'id_assign.py',
        usage = 'python id_assign.py [options] server ver',
    )
    parser.add_argument('-s', '--server', help = 'tst/prd')
    parser.add_argument('-v', '--version', help = 'data release version')
    options = parser.parse_args()
    if not options.server or not options.version:
        parser.print_help()
        sys.exit(1)
    server = options.server
    if server.lower() == 'prd':
        print('Cannot run this script on prd server.')
        sys.exit(1)
    data_ver = options.version

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
    logging.info(f'Loading data for data release version: {data_ver} #####################')

    ### setup first run hash_value index 
    misc_fns.setup_index(dbh, 'hash_value', id_collection, 'hash_value_1')

    ### initiate id assignment logic 
    data_release_glob_pattern = f'{data_root_path}/generated/datamodel/new_data/current/*.json'
    for fp in glob.glob(data_release_glob_pattern):
        data = misc_fns.load_json(fp)
        updated_data = process_data(data, dbh, id_collection, fp)
        misc_fns.write_json(fp, updated_data)
    
    logging.info('Finished loading data for data release version: {data_ver} #####################')
        
if __name__ == '__main__':
    main()