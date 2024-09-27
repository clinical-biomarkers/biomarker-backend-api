import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config
from tutils.db import get_standard_db_handle


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    config_obj = get_config()
    db_name = config_obj["dbinfo"]["dbname"]
    db_user = config_obj["dbinfo"][db_name]["user"]
    db_pass = config_obj["dbinfo"][db_name]["password"]

    ### get database handle and create the db user
    dbh = get_standard_db_handle(server=server)
    dbh.command(
        "createUser", db_user, pwd=db_pass, roles=[{"role": "readWrite", "db": db_name}]
    )


if __name__ == "__main__":
    main()
