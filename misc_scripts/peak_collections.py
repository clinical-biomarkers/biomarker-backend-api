"""Peak into a collection.

usage: parser.py [-h] [--biomarker_collection] [--canonical_id_map_collection]
                 [--second_id_map_collection] [--unreviewed_collection] 
                 [--request_log_collection] [--error_log_collection] [--search_cache] 
                 [-n NUM] server

positional arguments:
  server                prd/beta/tst/dev

options:
  -h, --help                        show this help message and exit
  --biomarker_collection            Store true argument for the biomarker collection.
  --canonical_id_map_collection     Store true argument for the canonical id map collection.
  --second_id_map_collection        Store true argument for the second id map collection.
  --unreviewed_collection           Store true argument for the unreviewed collection.
  --request_log_collection          Store true argument for the request log collection.
  --error_log_collection            Store true argument for the error log collection.
  --search_cache                    Store true argument for the search cache collection.
  -n NUM, --num NUM
"""

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import get_standard_db_handle, get_collections
from tutils.parser import standard_parser, parse_server

COLLECTION_LIST = list(get_collections().values())


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
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    option_dict = options.__dict__
    option_list = [val for val in option_dict.values() if isinstance(val, bool)]
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
