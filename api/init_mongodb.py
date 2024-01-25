import sys 
import argparse
from misc_functions import load_json, get_mongo_handle
import pymongo 

def main():

    ### handle command line arguments
    parser = argparse.ArgumentParser(
        prog = 'init_mongodb.py',
        usage = 'python init_mongodb.py [options] server'
    )
    parser.add_argument('-s', '--server', help = 'tst/prd')
    options = parser.parse_args()
    if not options.server or options.server not in {'tst', 'prd'}:
        parser.print_help()
        sys.exit(1)
    server = options.server 

    ### get config info for database connection
    config_obj = load_json('config.json')
    mongo_port = config_obj['dbinfo']['port'][server]
    host = f'mongodb://127.0.0.1:{mongo_port}'
    admin_user = config_obj['dbinfo']['admin']['user']
    admin_pass = config_obj['dbinfo']['admin']['password']
    admin_db = config_obj['dbinfo']['admin']['db']
    db_name = config_obj['dbinfo']['dbname']
    db_user = config_obj['dbinfo'][db_name]['user']
    db_pass = config_obj['dbinfo'][db_name]['password']

    ### get database handle and create the db user 
    dbh = get_mongo_handle(host, admin_db, admin_user, admin_pass, db_name)
    dbh.command('createUser', db_user, pwd = db_pass, roles = [{'role': 'readWrite', 'db': db_name}])

if __name__ == '__main__':
    main() 