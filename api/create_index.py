"""Creates a text index on the biomarker collection.
"""

import pymongo
import sys
import argparse
import misc_functions as misc_fns


def main():

    parser = argparse.ArgumentParser(
        prog="create_text_index.py",
    )
    parser.add_argument("-s", "--server", help="tst/prd")
    options = parser.parse_args()
    if not options.server:
        parser.print_help()
        sys.exit(0)
    server = options.server
    if server.lower() not in {"tst", "prd"}:
        print('Invalid server name. Excepcts "tst" or "prd"')
        sys.exit(0)

    _, host, db_name, db_user, db_pass, data_collection, _, _, _ = (
        misc_fns.get_config_details(server)
    )

    try:
        client = pymongo.MongoClient(
            host,
            username=db_user,
            password=db_pass,
            authSource=db_name,
            authMechanism="SCRAM-SHA-1",
            serverSelectionTimeoutMS=10000,
        )
        client.server_info()
        dbh = client[db_name]
        biomarker_collection = dbh[data_collection]
        # TODO : this is just for first run to delete wildcard index, delete after
        # result = biomarker_collection.create_index([("$**", pymongo.TEXT)])
        biomarker_collection.drop_index("$**_text")
        result = biomarker_collection.create_index([("all_text", "text")])
        print(result)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
