"""Creates the necessary indexes on the biomarker and search collections.
"""

import pymongo
from pymongo.database import Database
import sys
import argparse
from typing import Optional
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

    (
        _,
        host,
        db_name,
        db_user,
        db_pass,
        biomarker_collection_name,
        _,
        _,
        _,
        search_collection_name,
    ) = misc_fns.get_config_details(server)

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

        biomarker_collection_handle = dbh[biomarker_collection_name]
        biomarker_existing_indexes = biomarker_collection_handle.index_information()
        biomarker_index_keys = {"biomarker_id_1": [("biomarker_id", pymongo.ASCENDING)]}
        for index_name, index_key in biomarker_index_keys.items():
            if index_name in biomarker_existing_indexes:
                print(
                    f"The index `{index_name}` for collection `{biomarker_collection_name}` already exists."
                )
            else:
                result = biomarker_collection_handle.create_index(
                    index_key, name=index_name, unique=True
                )
                print(
                    f"Created index `{result}` for collection `{biomarker_collection_name}`."
                )

        search_collection_handle = dbh[search_collection_name]
        search_existing_keys = search_collection_handle.index_information()

        # TODO : one time execution delete this
        incorrect_index_name = "all_text_text"
        incorrect_index_details = {"v": 2, "key": [("all_text", 1), ("text", 1)]}
        if incorrect_index_name in search_existing_keys:
            if search_existing_keys[incorrect_index_name] == incorrect_index_details:
                search_collection_handle.drop_index(incorrect_index_name)
                print(
                    f"Dropped incorrect index `{incorrect_index_name}` from collection `{search_collection_name}`."
                )

        search_index_keys = {
            "all_text_text": [("all_text", pymongo.TEXT)],
            "biomarker_id_1": [("biomarker_id", pymongo.ASCENDING)],
            "biomarker_id_-1": [("biomarker_id", pymongo.DESCENDING)],
            "assessed_biomarker_entity_1": [
                (
                    "assessed_biomarker_entity",
                    pymongo.ASCENDING,
                )
            ],
            "assessed_biomarker_entity_-1": [
                (
                    "assessed_biomarker_entity",
                    pymongo.DESCENDING,
                )
            ],
        }
        for index_name, index_key in search_index_keys.items():
            if index_name in search_existing_keys:
                print(
                    f"The index `{index_name}` for collection `{search_collection_name}` already exists."
                )
            else:
                if "biomarker_id" in index_name:
                    result = search_collection_handle.create_index(
                        index_key, name=index_name, unique=True
                    )
                else:
                    result = search_collection_handle.create_index(
                        index_key, name=index_name
                    )
                print(
                    f"Created index `{result}` for collection `{search_collection_name}`."
                )

    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
