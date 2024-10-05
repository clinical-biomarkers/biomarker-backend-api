import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config
from tutils.db import get_connection_string, load_id_collection


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    config_obj = get_config()
    data_root_path = config_obj["data_path"]
    generated_path_segment = config_obj["generated_path_segment"]
    db_name = config_obj["dbinfo"]["dbname"]
    canonical_id_collection = config_obj["dbinfo"][db_name]["canonical_id_map"]
    second_level_id_collection = config_obj["dbinfo"][db_name]["second_level_id_map"]

    id_collections = {
        "canonical": {
            "path": os.path.join(
                data_root_path, *generated_path_segment, "canonical_id_collection.json"
            ),
            "name": canonical_id_collection,
        },
        "second_level": {
            "path": {
                os.path.join(
                    data_root_path,
                    *generated_path_segment,
                    "second_level_id_collection.json",
                )
            },
            "name": second_level_id_collection,
        },
    }

    connection_string = get_connection_string(server=server)

    for collection, data in id_collections.items():
        if load_id_collection(
            connection_string=connection_string,
            load_path=data["path"],
            collection=data["name"],
        ):
            print(f"Successfully loaded {collection}.")
        else:
            print(f"Something went wrong loading {collection}.")


if __name__ == "__main__":
    main()
