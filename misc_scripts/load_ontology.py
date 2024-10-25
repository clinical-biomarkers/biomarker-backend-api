import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config
from tutils.db import get_standard_db_handle
from tutils.general import load_json_type_safe


def load_ontology(server: str) -> None:

    config_obj = get_config()
    source_path = os.path.join(
        config_obj["data_path"], *config_obj["generated_path_segment"], "obci.json"
    )
    ontology_collection = config_obj["ontology_collection"]
    ontology_json = load_json_type_safe(filepath=source_path, return_type="list")
    document = {"data": ontology_json}

    dbh = get_standard_db_handle(server=server)
    del_result = dbh[ontology_collection].delete_many({})
    print(f"Deleted {del_result.deleted_count}")
    result = dbh[ontology_collection].insert_one(document)
    print(f"Document inserted with _id: {result.inserted_id}")


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    load_ontology(server=server)


if __name__ == "__main__":
    main()
