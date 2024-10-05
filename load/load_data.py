"""Loads the preprocessed data into MongoDB.

usage: parser.py [-h] server

positional arguments:
  server      prd/beta/tst/dev

options:
  -h, --help  show this help message and exit
"""

import glob
import sys
import time
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import (
    get_standard_db_handle,
    get_connection_string,
    setup_index,
    create_text_index,
    load_id_collection,
)
from tutils.config import get_config
from tutils.general import load_json_type_safe, resolve_symlink, get_user_confirmation
from tutils.parser import standard_parser
from tutils.logging import setup_logging, log_msg, start_message
from load.load_utils import (
    clear_collections,
    create_load_record_command,
    bulk_load,
    process_stats,
)
from load.preprocess import CHECKPOINT_VAL

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

    start_message(logger=LOGGER, msg=f"Loading data for server: {server}")

    config_obj = get_config()
    db_name = config_obj["dbinfo"]["dbname"]
    biomarker_collection = config_obj["dbinfo"][db_name]["collection"]
    stats_collection = config_obj["dbinfo"][db_name]["stats_collection"]
    data_root_path_segment = config_obj["data_path"]
    generated_path_segment = config_obj["generated_path_segment"]
    merged_data_path_segment = config_obj["merged_data_segment"]
    canonical_id_collection = config_obj["dbinfo"][db_name]["canonical_id_map"]
    second_level_id_collection = config_obj["dbinfo"][db_name]["second_level_id_map"]

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

    paths = [
        "biomarker_component.biomarker",
        "biomarker_component.assessed_biomarker_entity.recommended_name",
        "biomarker_component.assessed_biomarker_entity_id",
        "biomarker_component.assessed_entity_type",
        "condition.recommended_name.name",
        "best_biomarker_role.role",
    ]
    log_msg(logger=LOGGER, msg="Attempting to create indexes...")
    setup_index(
        collection=dbh[biomarker_collection],
        index_field="biomarker_id",
        unique=True,
        index_name="biomarker_id_1",
        logger=LOGGER,
    )
    for path in paths:
        setup_index(
            collection=dbh[biomarker_collection],
            index_field=path,
            index_name=f"{path}_1",
            unique=False,
            order="ascending",
        )
        setup_index(
            collection=dbh[biomarker_collection],
            index_field=path,
            index_name=f"{path}_-1",
            unique=False,
            order="descending",
        )
    create_text_index(collection=dbh[biomarker_collection], logger=LOGGER)

    if server != "dev":
        canonical_id_collection_local_path = os.path.join(
            data_root_path_segment,
            *generated_path_segment,
            "canonical_id_collection.json",
        )
        second_level_id_collection_local_path = os.path.join(
            data_root_path_segment,
            *generated_path_segment,
            "second_level_id_collection.json",
        )
        connection_string = get_connection_string(server=server)
        if load_id_collection(
            connection_string=connection_string,
            load_path=canonical_id_collection_local_path,
            collection=canonical_id_collection,
        ):
            log_msg(
                logger=LOGGER,
                msg="Successfully loaded canonical ID map.",
                to_stdout=True,
            )
        else:
            log_msg(
                logger=LOGGER,
                msg="Failed loading canonical ID map. You will have to update manually.",
                level="error",
                to_stdout=True,
            )
        if load_id_collection(
            connection_string=connection_string,
            load_path=second_level_id_collection_local_path,
            collection=second_level_id_collection,
        ):
            log_msg(
                logger=LOGGER,
                msg="Successfully loaded second level ID map.",
                to_stdout=True,
            )
        else:
            log_msg(
                logger=LOGGER,
                msg="Failed loading second level ID map. You will have to update manually.",
                level="error",
                to_stdout=True,
            )

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

    log_msg(logger=LOGGER, msg="Calculating metadata stats...", to_stdout=True)
    stats_start_time = time.time()
    process_stats(
        dbh=dbh, data_collection=biomarker_collection, stat_collection=stats_collection
    )
    stats_elapsed_time = round(time.time() - stats_start_time, 2)
    log_msg(
        logger=LOGGER,
        msg=f"Finished calculating stats in {stats_elapsed_time} seconds.",
        to_stdout=True,
    )

    finish_str = "Finished loading data and calculating new metadata stats."
    finish_str += f"\n\tClearing old data took {clear_collection_elapsed_time} seconds."
    finish_str += f"\n\tLoading merged data took {merged_elapsed_time} seconds."
    finish_str += f"\n\tLoading collision data took {collision_elapsed_time} seconds."
    finish_str += f"\n\tCalculating stats took {stats_elapsed_time} seconds."
    finish_str += f"\n\tTotal time: {clear_collection_elapsed_time + merged_elapsed_time + collision_elapsed_time + stats_elapsed_time} seconds."
    log_msg(logger=LOGGER, msg=finish_str, to_stdout=True)


if __name__ == "__main__":
    main()
