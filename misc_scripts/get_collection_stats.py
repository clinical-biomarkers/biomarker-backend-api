"""Get some metadata stats for all the collections.

usage: parser.py [-h] server

positional arguments:
  server      prd/beta/tst/dev

options:
  -h, --help  show this help message and exit
"""

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import get_standard_db_handle, get_collections
from tutils.parser import standard_parser, parse_server

COLLECTION_LIST = list(get_collections().values())


def main():

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

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
