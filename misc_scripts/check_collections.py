import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import get_standard_db_handle
from tutils.parser import standard_parser, parse_server


def main():

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    dbh = get_standard_db_handle(server=server)

    try:
        collections = dbh.list_collection_names()
        print("Collections:")
        for collection in collections:
            print(f"- {collection}")
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
