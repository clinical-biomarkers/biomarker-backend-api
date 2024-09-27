"""DEPRECATED

Creates a text index on the biomarker collection.
"""

import sys
import os

# if trying to use this script, have to edit this to include another ".."
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import get_standard_db_handle, create_text_index
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config


def main():

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    config_obj = get_config()
    db_name = config_obj["dbinfo"]["dbname"]
    data_collection = config_obj["dbinfo"][db_name]["collection"]

    dbh = get_standard_db_handle(server=server)
    biomarker_collection = dbh[data_collection]
    try:
        create_text_index(collection=biomarker_collection)
        result = biomarker_collection.create_index([("all_text", "text")])
        print(result)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
