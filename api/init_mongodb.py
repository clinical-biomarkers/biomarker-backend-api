import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config
from tutils.db import get_database_handle


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    config_obj = get_config()
    db_name = config_obj["dbinfo"]["dbname"]
    port = config_obj["dbinfo"]["port"][server]
    admin_user = config_obj["dbinfo"]["admin"]["user"]
    admin_pass = config_obj["dbinfo"]["admin"]["password"]
    admin_db = config_obj["dbinfo"]["admin"]["db"]
    db_user = config_obj["dbinfo"][db_name]["user"]
    db_pass = config_obj["dbinfo"][db_name]["password"]

    ### get database handle and create the db user
    dbh = get_database_handle(db_name=db_name, port=port, username=admin_user, password=admin_pass, auth_source=admin_db)
    dbh.command(
        "createUser", db_user, pwd=db_pass, roles=[{"role": "readWrite", "db": db_name}]
    )


if __name__ == "__main__":
    main()
