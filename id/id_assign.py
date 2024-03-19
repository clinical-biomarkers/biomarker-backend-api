import logging 
import argparse 
import sys
import glob 
from helpers import id_backend as id_backend
from helpers import misc_functions as misc_fns

# batch size for mongodb bulk write operations
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
    config_obj = misc_fns.load_json('../api/config.json')
    if not isinstance(config_obj, dict):
        print(f"Error reading config JSON, expected type `dict`, got {type(config_obj)}")
        sys.exit(1)
    mongo_port = config_obj['dbinfo']['port'][server]
    host = f'mongodb://127.0.0.1:{mongo_port}'
    _db_name = config_obj['dbinfo']['dbname']
    db_user = config_obj['dbinfo'][_db_name]['user']
    db_pass = config_obj['dbinfo'][_db_name]['password']
    data_root_path = config_obj['data_path']
    canonical_id_collection = config_obj['dbinfo'][_db_name]['canonical_id_map']
    second_level_id_collection = config_obj['dbinfo'][_db_name]['second_level_id_map']
    data_collection = config_obj['dbinfo'][_db_name]['collection']
    unreviewed_collection = config_obj['dbinfo'][_db_name]['unreviewed_collection']
    dbh = misc_fns.get_mongo_handle(host, _db_name, db_user, db_pass)

    ### setup logger
    misc_fns.setup_logging('./logs/id_assign.log')
    logging.info('Beginning ID assignment process #####################')

    ### setup first run hash_value index 
    misc_fns.setup_index(dbh, 'hash_value', canonical_id_collection, 'hash_value_1')
    misc_fns.setup_index(dbh, 'biomarker_canonical_id', second_level_id_collection, 'canonical_1')

    ### initiate id assignment logic 
    data_release_glob_pattern = f'{data_root_path}/generated/datamodel/new_data/current/*.json'
    canonical_id_collection_local_path = f'{data_root_path}/generated/datamodel/canonical_id_collection.json'
    second_level_id_collection_local_path = f'{data_root_path}/generated/datamodel/second_level_id_collection.json'

    for fp in glob.glob(data_release_glob_pattern):
        data = misc_fns.load_json(fp)
        if not isinstance(data, list):
            logging.error(f"Error reading data from file `{fp}`, expected `list`, got `{type(data)}`.")
            print(f"Error reading data from file `{fp}`, expected `list`, got `{type(data)}`. Skipping...")
            continue
        updated_data = id_backend.process_file_data(
            data, dbh, fp, 
            canonical_id_collection_local_path,
            second_level_id_collection_local_path,
            data_collection, unreviewed_collection, 
            canonical_id_collection, second_level_id_collection, 
        )
        if not updated_data:
            logging.error(f"Error reading data from file `{fp}`, expected `list`, got `{type(data)}`.")
            print(f"Error reading data from file `{fp}`, expected `list`, got `{type(data)}`. Skipping...")
            continue
        misc_fns.write_json(fp, updated_data)
        
    logging.info('Finished ID assignment process ---------------------')
        
if __name__ == '__main__':
    main()
