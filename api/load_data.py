import sys
import json
import csv 
import pymongo 
from optparse import OptionParser

def main(): 
    
    # defines the format of the command line prompt 
    usage = '%prog [options]' 
    # option parser 
    parser = OptionParser(usage)
    # add command line options 
    # -s or --server, takes the values of dev, tst, beta, or prd
    parser.add_option('-s', '--server', action = 'store', dest = 'server', help = 'dev/tst/beta/prd')
    # -f or --file, takes the filepath to the input file 
    parser.add_option('-f', '--file', action = 'store', dest = 'csvfile', help = 'Filepath of the input CSV file')
    # parse the command line arguments 
    (options, _) = parser.parse_args() 

    # check the input arguments 
    if not options.server or not options.csvfile:
        parser.print_help() 
        sys.exit(1)
    server = options.server
    fp = options.csvfile 

    # read in config file
    with open('config.json', 'r') as f:
        config_obj = json.load(f)

    # mongo port 
    mongo_port = config_obj['dbinfo']['port'][server]
    # host 
    host = f'mongodb://127.0.0.1:{mongo_port}'
    # database 
    db_name = config_obj['dbinfo']['dbname']
    # collection 
    db_collection = config_obj['dbinfo'][db_name]['collection']
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

    # open the input csv file and populate the database collection 
    # TODO rework this logic to bulk load 
    with open(fp, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dbh[db_collection].insert_one(row)

if __name__ == '__main__': 
    main() 

