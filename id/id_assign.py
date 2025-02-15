import argparse
import sys
import glob
import os
from time import time
from helpers import id_backend as id_backend
from helpers import LOGGER

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import (
    get_standard_db_handle,
    setup_index,
    get_connection_string,
    dump_id_collection,
)
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config
from tutils.general import (
    load_json_type_safe,
    get_user_confirmation,
    resolve_symlink,
    write_json,
)
from tutils.logging import log_msg, start_message
from tutils.constants import (
    canonical_id_default,
    second_level_id_default,
)


def main() -> None:

    parser = argparse.ArgumentParser(
        prog="id_assign.py",
        usage="python id_assign.py [options] server",
    )
    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)
    if server.lower() != "dev":
        print("Can only run this script on the `dev` server.")
        sys.exit(1)

    start_message(logger=LOGGER, msg="Beginning ID assignment process.")

    config_obj = get_config()

    data_root_path = config_obj["data_path"]
    generated_path_segment = config_obj["generated_path_segment"]
    new_data_segment = config_obj["new_data_segment"]

    canonical_id_collection = canonical_id_default()
    second_level_id_collection = second_level_id_default()

    dbh = get_standard_db_handle(server=server)

    ### setup first run index
    setup_index(
        collection=dbh[canonical_id_collection],
        index_field="hash_value",
        unique=True,
        index_name="hash_value_1",
    )
    setup_index(
        collection=dbh[second_level_id_collection],
        index_field="biomarker_canonical_id",
        unique=True,
        index_name="canonical_1",
    )

    ### initiate id assignment logic
    new_data_dir_path = os.path.join(
        data_root_path, *generated_path_segment, *new_data_segment
    )
    resolved_symlink = resolve_symlink(new_data_dir_path)
    print(f"Resolved symlink for {new_data_dir_path} point to:\n\t{resolved_symlink}")
    get_user_confirmation()

    data_release_glob_pattern = os.path.join(new_data_dir_path, "*.json")
    files = glob.glob(data_release_glob_pattern)
    files.sort()

    start_time = time()
    for fp in files:
        data = load_json_type_safe(filepath=fp, return_type="list")
        updated_data = id_backend.process_file_data(
            data=data,
            dbh=dbh,
            filepath=fp,
            can_id_coll=canonical_id_collection,
            second_id_coll=second_level_id_collection,
        )
        if not updated_data:
            log_msg(
                logger=LOGGER,
                msg=f"Error reading data from file `{fp}`. Updated data returned empty: {updated_data}, skipping...",
                level="warning",
            )
            continue
        write_json(fp, updated_data)

    local_save_path = os.path.join(data_root_path, *generated_path_segment)
    canonical_id_collection_local_path = os.path.join(
        local_save_path, "canonical_id_collection.json"
    )
    second_level_id_collection_local_path = os.path.join(
        local_save_path, "second_level_id_collection.json"
    )

    connection_string = get_connection_string(server=server)
    if dump_id_collection(
        connection_string=connection_string,
        save_path=canonical_id_collection_local_path,
        collection=canonical_id_collection,
    ):
        log_msg(
            logger=LOGGER, msg="Successfully dumped canonical ID map.", to_stdout=True
        )
    else:
        log_msg(
            logger=LOGGER,
            msg="Failed dumping canonical ID map. You will have to update manually.",
            level="error",
            to_stdout=True,
        )
    if dump_id_collection(
        connection_string,
        second_level_id_collection_local_path,
        second_level_id_collection,
    ):
        log_msg(
            logger=LOGGER,
            msg="Successfully dumped second level ID map.",
            to_stdout=True,
        )
    else:
        log_msg(
            logger=LOGGER,
            msg="Failed dumping second level ID map. You will have to update manually.",
            level="error",
            to_stdout=True,
        )

    elapsed_time = time() - start_time
    log_msg(
        logger=LOGGER,
        msg=f"Finished ID assignment process ({elapsed_time} seconds) ---------------------",
        to_stdout=True,
    )


if __name__ == "__main__":
    main()
