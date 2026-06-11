import os,sys
import string
from optparse import OptionParser
import glob
import json
from bson import json_util
import pymongo
from pymongo import MongoClient
import datetime


__version__="1.0"
__status__ = "Dev"



###############################
def main():


    usage = "\n%prog  [options]"
    parser = OptionParser(usage,version="%prog version___")
    parser.add_option("-s","--server",action="store",dest="server",help="dev/tst/beta/prd")
    parser.add_option("-c","--coll",action="store",dest="coll",help="") 
    (options,args) = parser.parse_args()

    for key in ([options.server, options.coll]):
        if not (key):
            parser.print_help()
            sys.exit(0)

    server = options.server
    coll = options.coll


    db_name = "biomarkerdb_api"
    mongo_port = "7071"
    host = "mongodb://127.0.0.1:%s" % (mongo_port)
    db_user, db_pass =  "biomarkeradmin", "biomarkerpass"

            
    try:
        client = pymongo.MongoClient(host,
            username=db_user,
            password=db_pass,
            authSource=db_name,
            authMechanism='SCRAM-SHA-1',
            serverSelectionTimeoutMS=10000
        )
        client.server_info()
        dbh = client[db_name]
        #q = {"glytoucan_ac": "G17689DH"}
        #q = { "phraselist": {"$eq": "diagnostic"},"record_type": {"$eq": "protein"}}
        q = {}
        for doc in dbh[coll].find(q):
            if "_id" in doc:
                doc.pop("_id")
            print (json.dumps(doc, indent=4))

            #print (str(doc["_id"]), doc["visibility"])
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print (err)
    except pymongo.errors.OperationFailure as err:
        print (err)



if __name__ == '__main__':
    main()
