from pymongo.collection import Collection
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import get_standard_db_handle, get_collection_list
from tutils.parser import standard_parser, parse_server

COLLECTION_LIST = get_collection_list()


def print_indexes(collection: Collection):
    """Format prints the indexes for the target collection."""
    try:
        indexes = collection.index_information()
        for name, index in indexes.items():
            print(f"Index name: {name}")
            print(f"Index details: {index}")
            print()
    except Exception as e:
        print(f"Failed to retrieve indexes: {e}")
        sys.exit(1)


def main():

    parser, server_list = standard_parser()
    for collection in COLLECTION_LIST:
        help_str = (
            f"Store true argument for the {collection.replace('_', ' ')} collection."
            if "collection" not in collection
            else f"Store true argument for the {collection.replace('_', ' ')}."
        )
        parser.add_argument(
            f"--{collection}",
            action="store_true",
            help=help_str,
        )
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    option_list = [key for key in options.__dict__.values()]
    if not any(option_list):
        print("Need to specify one collection.")
        parser.print_help()
        sys.exit(1)

    true_list = [x for x in option_list if x]
    if len(true_list) > 1:
        print("Too many collections passed, can only use one at a time.")
        parser.print_help()
        sys.exit(0)

    dbh = get_standard_db_handle(server=server)
    try:
        target_collection_idx = option_list.index(True)
        target_collection = COLLECTION_LIST[target_collection_idx]
        collection = dbh[target_collection]
        print_indexes(collection)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
