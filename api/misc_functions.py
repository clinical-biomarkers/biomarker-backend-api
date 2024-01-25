import logging
import json
import pymongo

def load_json(filepath: str) -> dict | list:
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
    with open(filepath, 'r') as f:
        json_obj = json.load(f)
    return json_obj

def write_json(filepath: str, data: dict | list) -> None:
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

def get_mongo_handle(host: str, authSource: str, username: str, password: str, db_name: str = None, authMechanism: str = 'SCRAM-SHA-1', serverSelectionTimeoutMS: int = 10000) -> pymongo.MongoClient:
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
        client = pymongo.MongoClient(host,
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

def setup_index(dbh, index_col: str, collection_name: str, index_name: str = None) -> None:
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