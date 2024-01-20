import sys
import json
import glob
import os
import pymongo 
from pymongo.errors import DuplicateKeyError
from id import *
from optparse import OptionParser
import logging 
from datetime import datetime
import copy

BATCH_SIZE = 1000

def process_data(data: dict, dbh, db_collection: str, id_collection: str, fp: str) -> tuple:
    ''' Processes the data for the current data file and inserts into the database if applicable.

    Parameters
    ----------
    data: dict
        The data to process.
    dbh: pymongo.MongoClient
        The database handle.
    db_collection: str
        The name of the collection to insert the data into.
    id_collection: str
        The name of the collection to check for hash collisions.
    fp: str
        The filepath to the data file.
    
    Returns
    -------
    tuple
        The updated data with the new biomarker ids and a message indicating the status of the insert operation.
    '''
    bulk_ops = []
    output_messages = []
    collisions = {}
    collision_count = 1 
    collision_report_filename = f'{os.path.splitext(os.path.split(fp)[1])[0]}_collisions.json'
    collision_report_path = f'./collision_reports/{collision_report_filename}'
    return_data = []

    # iterate over entries in the data
    for document in data:
        # generate hash value for data record 
        hash_value, core_values_str = generate_custom_id(document)
        # if there is a hash collision, don't add and add to output messages
        if check_collision(hash_value, dbh, id_collection):
            output_message = f'\nCollision detected for record in:\n\tFile: {fp}:\n\tDocument: {document}\n\tCore Values Str: {core_values_str}\n\tHash Value: {hash_value}\n'
            collisions[collision_count] = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'file': fp,
                'core_values_str': core_values_str,
                'hash_value': hash_value,
                'document': document
            }
            collision_count += 1
            print(output_message)
            output_messages.append(output_message)
        else: 
            biomarker_id = add_hash_and_increment_ordinal(hash_value, core_values_str, dbh, id_collection)
            document['biomarker_id'] = biomarker_id
            return_data.append(document)
            # add insert operation to bulk operations list
            document_copy = copy.deepcopy(document)
            bulk_ops.append(pymongo.InsertOne(document_copy))
        
        # if bulk operations list is full, execute the bulk write to avoid memory issues
        try:
            if len(bulk_ops) >= BATCH_SIZE:
                dbh[db_collection].bulk_write(bulk_ops, ordered = False)
                bulk_ops = []
        except DuplicateKeyError as e:
            print(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
            logging.error(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
    
    # execute the remaining bulk operations
    if bulk_ops:
        try:
            dbh[db_collection].bulk_write(bulk_ops)
        except DuplicateKeyError as e:
            print(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
            logging.error(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
    
    if not output_messages:
        return return_data, f'Successfully inserted all data records with no collisions for the file: {fp}.' 
    else:
        with open(collision_report_path, 'w') as f:
            json.dump(collisions, f, indent = 4)
        return return_data, '\n'.join(output_messages) + f'\nWriting collision report to: {collision_report_path}.'

def process_prd_data(data: dict, dbh, db_collection: str, fp: str) -> str:
    ''' Inserts the data into the prd database.

    Parameters
    ----------
    data: dict
        The data to process.
    dbh: pymongo.MongoClient
        The database handle.
    db_collection: str
        The name of the collection to insert the data into.
    fp: str
        The filepath to the data file.
    
    Returns
    -------
    str
        A message indicating the status of the insert operation.
    '''
    bulk_ops = []
    output_messages = []

    # iterate over entries in the data
    for document in data:
        # check that the document has a valid formatted biomarker id
        if not validate_id_format(document['biomarker_id']):
            output_message = f'\nInvalid biomarker id detected for record in:\n\tFile: {fp}:\n\tDocument: {document}\n'
            print(output_message)
            output_messages.append(output_message)
        else:
            bulk_ops.append(pymongo.InsertOne(document))
            try:
                if len(bulk_ops) >= BATCH_SIZE:
                    dbh[db_collection].bulk_write(bulk_ops)
                    bulk_ops = []
            except DuplicateKeyError as e:
                print(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
                logging.error(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
    
    if bulk_ops:
        try:
            dbh[db_collection].bulk_write(bulk_ops)
        except DuplicateKeyError as e:
            print(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
            logging.error(f'\nDuplicate key error:\n\tFile: {fp}\n\tError: {e}.')
    
    if not output_messages:
        return f'Successfully inserted all data records for the file: {fp}.'
    else:
        return '\n'.join(output_messages)

def setup_logging(log_path: str) -> None:
    ''' Configures the logger to write to a file.

    Parameters
    ----------
    log_path: str
        The path to the log file.
    '''
    logging.basicConfig(filename = log_path, level = logging.DEBUG, 
                        format = '%(asctime)s %(levelname)s %(message)s')

def main(): 
    
    # defines the format of the command line prompt 
    usage = '%prog [options]' 
    # option parser 
    parser = OptionParser(usage)
    # add command line options 
    # -s or --server, takes the values of dev, tst, beta, or prd
    parser.add_option('-s', '--server', action = 'store', dest = 'server', help = 'dev/tst/beta/prd')
    # -f or --file, takes the filepath to the input file 
    parser.add_option('-v', '--ver', action = 'store', dest = 'release_ver', help = 'data release version')
    # parse the command line arguments 
    (options, _) = parser.parse_args() 

    # check the input arguments 
    if not options.server or not options.release_ver:
        parser.print_help() 
        sys.exit(1)
    server = options.server
    data_ver = options.release_ver 

    # read in config file
    with open('config.json', 'r') as f:
        config_obj = json.load(f)

    # mongo port 
    mongo_port = config_obj['dbinfo']['port'][server]
    # host 
    host = f'mongodb://127.0.0.1:{mongo_port}'
    # database 
    db_name = config_obj['dbinfo']['dbname']
    # data root path 
    data_root_path = config_obj['data_path']
    # data collection 
    db_collection = config_obj['dbinfo'][db_name]['collection']
    # id collection
    id_collection = config_obj['dbinfo'][db_name]['id_collection']
    # database user info 
    db_user = config_obj['dbinfo'][db_name]['user']
    db_pass = config_obj['dbinfo'][db_name]['password']

    # establish database connection 
    try: 
        client = pymongo.MongoClient(host,
                                    username = db_user,
                                    password = db_pass,
                                    authSource = db_name,
                                    authMechanism = 'SCRAM-SHA-1',
                                    serverSelectionTimeoutMS = 10000) 
        # test the connection 
        client.server_info()
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        sys.exit(1)
    except pymongo.errors.OperationFailure as err:
        print(err)
        sys.exit(1)
    
    # get the database handle 
    dbh = client[db_name] 

    # create unique index on biomarker_id field
    biomarker_id_index = 'biomarker_id_1'
    if biomarker_id_index not in dbh[db_collection].index_information():
        dbh[db_collection].create_index('biomarker_id', name = biomarker_id_index, unique = True)

    # setup logging in current directory
    log_path = f'./collision_reports/load_data_{server}.log'
    setup_logging(log_path)
    logging.info(f'Loading data for server: {server} and data release version: {data_ver}. #####################')

    # glob pattern for JSON data model files 
    data_release_glob_pattern = f'{data_root_path}/releases/data/v-{data_ver}/datamodeldb/*.json'
    # process each file
    if server == 'tst':
        for fp in glob.glob(data_release_glob_pattern):
            with open(fp, 'r') as f:
                data = json.load(f)
                updated_data, output_message = process_data(data, dbh, db_collection, id_collection, fp)
                logging.info(output_message)
            with open(fp, 'w') as f:
                json.dump(updated_data, f, indent = 4)
    elif server == 'prd':
        for fp in glob.glob(data_release_glob_pattern):
            with open(fp, 'r') as f:
                data = json.load(f)
                output_message = process_prd_data(data, dbh, db_collection, fp)
                logging.info(output_message)
    
    logging.info(f'Finished loading data for server: {server} and data release version: {data_ver}. ---------------------')

if __name__ == '__main__': 
    main() 

