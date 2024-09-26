import sys
from utils.db import get_standard_db_handle
from utils.parser import standard_parser


def main():

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = options.server.lower().strip()
    if server not in server_list:
        print("Invalid server.")
        parser.print_help()
        sys.exit(1)

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
