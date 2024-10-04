"""Clears some supplementary collections. Allows you to clear all or one of the cache, 
log, and error collections.

usage: parser.py [-h] [-c] [-l] [-e] server

positional arguments:
  server       prd/beta/tst/dev

options:
  -h, --help   show this help message and exit
  -c, --cache  Store false argument for clearing the cache collection.
  -l, --log    Store false argument for clearing the log collection.
  -e, --error  Store false argument for clearing the error log collection.
"""
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import get_standard_db_handle
from tutils.config import get_config
from tutils.parser import standard_parser, parse_server
from tutils.general import get_user_confirmation


def main():

    parser, server_list = standard_parser()
    parser.add_argument(
        "-c",
        "--cache",
        action="store_false",
        help="Store false argument for clearing the cache collection.",
    )
    parser.add_argument(
        "-l",
        "--log",
        action="store_false",
        help="Store false argument for clearing the log collection.",
    )
    parser.add_argument(
        "-e",
        "--error",
        action="store_false",
        help="Store false argument for clearing the error log collection.",
    )
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    confimation_message = "Clearing the following collections:"
    if options.cache:
        confimation_message += "\n\tCache collection"
    if options.log:
        confimation_message += "\n\tLog collection"
    if options.error:
        confimation_message += "\n\tError collection"

    print(confimation_message)
    get_user_confirmation()

    dbh = get_standard_db_handle(server)
    config_obj = get_config()
    db_name = config_obj["dbinfo"]["dbname"]
    cache_collection = config_obj["dbinfo"][db_name]["cache_collection"]
    log_collection = config_obj["dbinfo"][db_name]["req_log_collection"]
    error_collection = config_obj["dbinfo"][db_name]["error_log_collection"]

    if options.cache:
        try:
            dbh[cache_collection].delete_many({})
            print("Cache collection cleared.")
        except Exception as e:
            print(f"Error clearing cache collection.\n{e}")

    if options.log:
        try:
            dbh[log_collection].delete_many({})
            print("Log collection cleared.")
        except Exception as e:
            print(f"Error clearing log collection.\n{e}")

    if options.error:
        try:
            dbh[error_collection].delete_many({})
            print("Error collection cleared.")
        except Exception as e:
            print(f"Error clearing error collection.\n{e}")


if __name__ == "__main__":
    main()
