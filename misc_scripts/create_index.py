"""Creates an index on the score field. Can be added to for other indexes."""

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import get_standard_db_handle, setup_index
from tutils.parser import standard_parser, parse_server


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    dbh = get_standard_db_handle(server)

    paths = ["score"]
    for path in paths:
        setup_index(
            collection=dbh["biomarker_collection"],
            index_field=path,
            index_name=f"{path}_1",
            unique=False,
            order="ascending",
        )
        setup_index(
            collection=dbh["biomarker_collection"],
            index_field=path,
            index_name=f"{path}_1",
            unique=False,
            order="descending",
        )


if __name__ == "__main__":
    main()
