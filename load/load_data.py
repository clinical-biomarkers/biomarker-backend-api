import glob
import sys
import time
import os
from load.preprocess import CHECKPOINT_VAL

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import get_standard_db_handle
from tutils.config import get_config
from tutils.general import load_json_type_safe, resolve_symlink, get_user_confirmation
from tutils.parser import standard_parser
from tutils.logging import setup_logging, log_msg
from load.load_utils import clear_collections, create_load_record_command, bulk_load

LOGGER = setup_logging("load_data.log")
WRITE_BATCH = 1_000


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    if not options.server:
        parser.print_help()
        sys.exit(1)
    server = options.server.lower().strip()
    if server not in server_list:
        print("Invalid server.")
        parser.print_help()
        sys.exit(1)

    log_msg(
        logger=LOGGER,
        msg=f"Loading data for server: {server}. #####################",
        to_stdout=True,
    )

    config_obj = get_config()
    data_root_path_segment = config_obj["data_path"]
    generated_path_segment = config_obj["generated_path_segment"]
    merged_data_path_segment = config_obj["merged_data_segment"]

    merged_path_root = os.path.join(
        data_root_path_segment, *generated_path_segment, *merged_data_path_segment
    )
    resolved_symlink = resolve_symlink(merged_path_root)
    print(f"Resolved symlink for {merged_path_root} points to:\n\t{resolved_symlink}")
    get_user_confirmation()

    merged_data_pattern = os.path.join(merged_path_root, "merged_json", "*.json")
    collision_data_pattern = os.path.join(merged_path_root, "collision_json", "*.json")

    merged_data_files = glob.glob(merged_data_pattern)
    collision_data_files = glob.glob(collision_data_pattern)

    dbh = get_standard_db_handle(server)
    log_msg(logger=LOGGER, msg="Clearing collections...")
    clear_collection_start_time = time.time()
    clear_collections(dbh=dbh)
    clear_collection_elapsed_time = round(time.time() - clear_collection_start_time, 2)
    log_msg(
        logger=LOGGER,
        msg=f"Finished clearing collections in {clear_collection_elapsed_time} seconds, ready to load.",
        to_stdout=True,
    )

    log_msg(
        logger=LOGGER,
        msg="------------- Starting merged data load -------------",
        to_stdout=True,
    )
    merged_start_time = time.time()
    merged_ops = []
    for idx, file in enumerate(merged_data_files):
        if idx + 1 % CHECKPOINT_VAL == 0:
            print(f"Hit merged data load checkpoint at index: {idx}")
        record = load_json_type_safe(filepath=file, return_type="dict")
        merged_ops.append(create_load_record_command(record=record, all_text=True))
        if len(merged_ops) == WRITE_BATCH:
            bulk_load(dbh=dbh, ops=merged_ops, destination="biomarker")
    if merged_ops:
        bulk_load(dbh=dbh, ops=merged_ops, destination="biomarker")
    merged_elapsed_time = round(time.time() - merged_start_time, 2)
    log_msg(
        logger=LOGGER,
        msg=f"Finished loading merged data in {merged_elapsed_time} seconds.",
        to_stdout=True,
    )

    log_msg(
        logger=LOGGER,
        msg="------------- Starting collision data load -------------",
        to_stdout=True,
    )
    collision_start_time = time.time()
    collision_ops = []
    for idx, file in enumerate(collision_data_files):
        if idx + 1 % CHECKPOINT_VAL == 0:
            print(f"Hit collision load checkpoint at index: {idx}")
        record = load_json_type_safe(filepath=file, return_type="dict")
        collision_ops.append(create_load_record_command(record=record, all_text=False))
        if len(collision_ops) == WRITE_BATCH:
            bulk_load(dbh=dbh, ops=collision_ops, destination="collision")
    if collision_ops:
        bulk_load(dbh=dbh, ops=collision_ops, destination="collision")
    collision_elapsed_time = round(time.time() - collision_start_time, 2)
    log_msg(
        logger=LOGGER,
        msg=f"Finished loading collision data in {collision_elapsed_time} seconds.",
        to_stdout=True,
    )

    finish_str = "Finished loading data."
    finish_str += f"\n\tClearing old data took {clear_collection_elapsed_time} seconds."
    finish_str += f"\n\tLoading merged data took {merged_elapsed_time} seconds."
    finish_str += f"\n\tLoading collision data took {collision_elapsed_time} seconds."
    finish_str += f"\n\tTotal time: {clear_collection_elapsed_time + merged_elapsed_time + collision_elapsed_time} seconds."
    log_msg(logger=LOGGER, msg=finish_str, to_stdout=True)


if __name__ == "__main__":
    main()
