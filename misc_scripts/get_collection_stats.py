import sys
from utils.db import get_standard_db_handle, get_collection_list
from utils.parser import standard_parser

COLLECTION_LIST = get_collection_list()


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
        for collection in COLLECTION_LIST:
            collection_handle = dbh[collection]
            document_count = collection_handle.count_documents({})
            stats = dbh.command("collstats", collection)
            print(f"{collection.upper()} Stats:")
            print(f"\tNumber of documents: {document_count}")
            print(f"\tCollection size (in bytes): {stats['size']}")
            print(f"\tAverage document size (in bytes): {stats['avgObjSize']}")
            print(f"\tStorage size (in bytes): {stats['storageSize']}")
            print(f"\tNumber of indexes: {stats['nindexes']}")
            print(f"\tTotal index size (in bytes): {stats['totalIndexSize']}")

    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
