import sys 
from optparse import OptionParser 
import json 
import pymongo 

def main():

    # defines the format of the command line prompt
    usage = '%prog [options]'
    # option parser 
    parser = OptionParser(usage)
    # add new command line option, either -s or --server, which takes values of dev, tst, beta, or prd
    # stores the server value in parser.server 
    parser.add_option('-s', '--server', action = 'store', dest = 'server', help = 'dev/tst/beta/prd')
    # parse the command line arguments 
    (options, _) = parser.parse_args()

    # check the input arguments
    if not options.server:
        parser.print_help()
        sys.exit(1)
    if options.server not in {'dev', 'tst', 'beta', 'prd'}:
        parser.print_help()
        sys.exit(1)
    server = options.server 

    # read in config file
    with open('config.json', 'r') as f:
        config_obj = json.load(f)

    # mongo database port 
    mongo_port = config_obj['dbinfo']['port'][server]
    # host 
    host = f'mongodb://127.0.0.1:{mongo_port}'

    # grab admin db username and password 
    admin_user = config_obj['dbinfo']['admin']['user']
    admin_pass = config_obj['dbinfo']['admin']['password']

    # admin database
    admin_db = config_obj['dbinfo']['admin']['db']

    # database name 
    db_name = config_obj['dbinfo']['dbname']

    # grab user username and password 
    db_user = config_obj['dbinfo'][db_name]['user']
    db_pass = config_obj['dbinfo'][db_name]['password']

    # establish the database connection and create the db user 
    try: 
        client = pymongo.MongoClient(host, 
                                    username = admin_user,
                                    password = admin_pass,
                                    authSource = admin_db,
                                    authMechanism = 'SCRAM-SHA-1',
                                    serverSelectionTimeoutMS = 10000)
        # test the connection 
        client.server_info() 
        # create db user 
        client[db_name].command('createUser', db_user, pwd = db_pass, roles = [{'role': 'readWrite', 'db': db_name}])
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
    except pymongo.errors.OperationFailure as err:
        print(err)

if __name__ == '__main__':
    main() 