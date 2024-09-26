import sys
from utils.db import get_standard_db_handle, get_collection_list
from utils.parser import standard_parser

COLLECTION_LIST = get_collection_list()


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
    parser.add_argument("-n", "--num", type=int, default=5)
    options = parser.parse_args()
    server = options.server.lower().strip()
    if server not in server_list:
        print("Invalid server.")
        parser.print_help()
        sys.exit(1)

    option_list = [key for key in options.__dict__.values()]
    if not any(option_list):
        print("Need to specify one collection.")
        parser.print_help()
        sys.exit(0)

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

        last_entries = collection.find().sort("_id", -1).limit(options.num)
        last_entries_list = list(last_entries)
        last_entries_list.reverse()

        for idx, entry in enumerate(last_entries_list):
            print(f"Entry: {idx + 1}:\n{entry}\n")
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
