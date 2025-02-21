import sys
import glob
import os
from time import time
from helpers import id_backend as id_backend
from helpers import LOGGER
from traceback import format_exc
from argparse import ArgumentParser

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
    confirmation_message_complete,
)
from tutils.logging import log_msg, start_message, elapsed_time_formatter
from tutils.constants import (
    canonical_id_default,
    second_level_id_default,
)
from tutils.notify import send_notification


def build_parser() -> tuple[ArgumentParser, list[str]]:
    parser, server_list = standard_parser()
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--email", action="append", required=False)
    return parser, server_list


def main(server: str) -> str:

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
        logger=LOGGER,
    )
    setup_index(
        collection=dbh[second_level_id_collection],
        index_field="biomarker_canonical_id",
        unique=True,
        index_name="canonical_1",
        logger=LOGGER,
    )

    ### initiate id assignment logic
    new_data_dir_path = os.path.join(
        data_root_path, *generated_path_segment, *new_data_segment
    )
    resolved_symlink = resolve_symlink(new_data_dir_path)
    print(f"Resolved symlink for {new_data_dir_path} point to:\n\t{resolved_symlink}")
    get_user_confirmation()

    confirmation_message_complete()

    data_release_glob_pattern = os.path.join(new_data_dir_path, "*.json")
    files = glob.glob(data_release_glob_pattern)
    files.sort()

    os.makedirs(name=id_backend.NEW_BIOMARKER_ID_LIST_DIR, exist_ok=True)

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
    dump_id_collection(
        connection_string=connection_string,
        save_path=canonical_id_collection_local_path,
        collection=canonical_id_collection,
        logger=LOGGER,
    )
    dump_id_collection(
        connection_string=connection_string,
        save_path=second_level_id_collection_local_path,
        collection=second_level_id_collection,
        logger=LOGGER,
    )

    elapsed_time = time() - start_time
    elapsed_time_str = elapsed_time_formatter(elapsed_time)
    msg = f"Elapsed time: {elapsed_time_str}\n" "Finished ID assignment process" + (
        "-" * 30
    )
    log_msg(
        logger=LOGGER,
        msg=msg,
    )

    return elapsed_time_str


if __name__ == "__main__":
    parser, server_list = build_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    if server.lower() != "dev":
        print("Can only run this script on the `dev` server.")
        sys.exit(1)
    if options.notify and not options.email:
        print("Notify was set to true but no emails were passed, see --help:\n")
        parser.print_help()
        sys.exit(1)

    subject: str
    message: str
    try:
        elapsed_time = main(server=server)
        subject = f"[SUCCESS] {server} ID Assign Process Completed"
        message = f"Elapsed time: {elapsed_time}\n"
    except Exception as e:
        subject = f"[Failed] {server} ID Assign Process Completed"
        message = f"ID Assign failed, check the logs.\n{e}\n{format_exc()}"
        log_msg(
            logger=LOGGER,
            msg=message,
            level="error",
        )
    if options.notify:
        send_notification(
            email=options.email,
            subject=subject,
            message=message,
            server=server,
            logger=LOGGER,
        )
