import glob
import sys
import time
import os
from traceback import format_exc
from argparse import Namespace

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.db import (
    get_standard_db_handle,
    get_connection_string,
    setup_index,
    create_text_index,
    load_id_collection,
)
from tutils.config import get_config
from tutils.general import (
    load_json_type_safe,
    resolve_symlink,
    get_user_confirmation,
    confirmation_message_complete,
)
from tutils.parser import standard_parser, notify_parser
from tutils.constants import (
    biomarker_default,
    stats_default,
    canonical_id_default,
    second_level_id_default,
)
from tutils.logging import log_msg, start_message, elapsed_time_formatter
from load.load_utils import (
    clear_collections,
    create_load_record_command,
    bulk_load,
    process_stats,
    LOGGER,
    load_checkpoint,
)
from tutils.notify import send_notification
from load.preprocess import CHECKPOINT_VAL

WRITE_BATCH = 500


def main(options: Namespace, server: str) -> str:

    start_message(logger=LOGGER, msg=f"Loading data for server: {server}")

    config_obj = get_config()
    biomarker_collection = biomarker_default()
    stats_collection = stats_default()
    data_root_path_segment = config_obj["data_path"]
    generated_path_segment = config_obj["generated_path_segment"]
    merged_data_path_segment = config_obj["merged_data_segment"]
    canonical_id_collection = canonical_id_default()
    second_level_id_collection = second_level_id_default()

    merged_path_root = os.path.join(
        data_root_path_segment, *generated_path_segment, *merged_data_path_segment
    )
    resolved_symlink = resolve_symlink(merged_path_root)
    print(f"Resolved symlink for {merged_path_root} points to:\n\t{resolved_symlink}")
    get_user_confirmation()

    merged_data_pattern = os.path.join(merged_path_root, "merged_json", "*.json")
    collision_data_pattern = os.path.join(merged_path_root, "collision_json", "*.json")

    merged_data_files = sorted(glob.glob(merged_data_pattern))
    collision_data_files = sorted(glob.glob(collision_data_pattern))
    log_msg(
        logger=LOGGER,
        msg=f"Found:\n\t{len(merged_data_files)} merged files\n\t{len(collision_data_files)} collision files",
    )

    start_index = -1
    if options.continue_load:
        start_index = load_checkpoint(server=server)
        if start_index >= 0:
            msg = f"Continuing from index {start_index + 1}"
            log_msg(logger=LOGGER, msg=msg)
            print(msg)
            get_user_confirmation()
        else:
            print("Fall back to full load?")
            get_user_confirmation()

    confirmation_message_complete()

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
            logger=LOGGER,
        )
        setup_index(
            collection=dbh[biomarker_collection],
            index_field=path,
            index_name=f"{path}_-1",
            unique=False,
            order="descending",
            logger=LOGGER,
        )
    create_text_index(collection=dbh[biomarker_collection], logger=LOGGER)

    loading_coll_log = ""
    canonical_id_load_time = 0.0
    second_id_load_time = 0.0
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
        canonical_id_load_time = load_id_collection(
            connection_string=connection_string,
            load_path=canonical_id_collection_local_path,
            collection=canonical_id_collection,
            logger=LOGGER,
        )
        second_id_load_time = load_id_collection(
            connection_string=connection_string,
            load_path=second_level_id_collection_local_path,
            collection=second_level_id_collection,
            logger=LOGGER,
        )
        loading_coll_log = (
            f"\tLoading ID collections took:\n"
            f"\t\tCanonical: {elapsed_time_formatter(canonical_id_load_time)}\n"
            f"\t\tSecond Level: {elapsed_time_formatter(second_id_load_time)}\n"
            f"\t\tTotal: {elapsed_time_formatter(canonical_id_load_time + second_id_load_time)}\n"
        )

    clear_collection_elapsed_time: float | str = "N/A"
    if start_index < 0:
        log_msg(logger=LOGGER, msg="Clearing collections...")
        clear_collection_start_time = time.time()
        clear_collections(dbh=dbh)
        clear_collection_elapsed_time = time.time() - clear_collection_start_time
        log_msg(
            logger=LOGGER,
            msg=f"Finished clearing collections in {elapsed_time_formatter(clear_collection_elapsed_time)}, ready to load.",
        )

    log_msg(
        logger=LOGGER,
        msg="------------- Starting merged data load -------------",
    )
    merged_start_time = time.time()
    merged_ops = []
    total_merged_ops = 0
    last_index = start_index

    for idx, file in enumerate(merged_data_files):
        if idx <= start_index:
            continue

        last_index = idx

        if (idx + 1) % CHECKPOINT_VAL == 0:
            log_msg(
                logger=LOGGER,
                msg=f"Hit merged data load checkpoint at index: {idx + 1}",
            )
        try:
            record = load_json_type_safe(filepath=file, return_type="dict")
        except Exception as e:
            msg = f"Error loading merged data on file: {file}\n{e}\n{format_exc()}"
            log_msg(logger=LOGGER, msg=msg, level="error")
            sys.exit(1)

        merged_ops.append(create_load_record_command(record=record, all_text=True))
        if len(merged_ops) == WRITE_BATCH:
            log_msg(logger=LOGGER, msg=f"Bulk writing at index: {idx + 1}.")
            bulk_load(
                dbh=dbh,
                ops=merged_ops,
                destination="biomarker",
                server=server,
                current_index=last_index,
            )
            total_merged_ops += len(merged_ops)
            merged_ops = []
    if merged_ops:
        log_msg(logger=LOGGER, msg="Writing leftover records...")
        bulk_load(
            dbh=dbh,
            ops=merged_ops,
            destination="biomarker",
            server=server,
            current_index=last_index,
        )
        total_merged_ops += len(merged_ops)
        merged_ops = []
    merged_elapsed_time = round(time.time() - merged_start_time, 2)
    log_msg(
        logger=LOGGER,
        msg=f"Finished loading merged data in {elapsed_time_formatter(merged_elapsed_time)}, completed {total_merged_ops} writes.",
    )

    log_msg(
        logger=LOGGER,
        msg="------------- Starting collision data load -------------",
    )
    collision_start_time = time.time()
    collision_ops = []
    total_collision_ops = 0
    for idx, file in enumerate(collision_data_files):
        if (idx + 1) % CHECKPOINT_VAL == 0:
            log_msg(logger=LOGGER, msg=f"Hit collision load checkpoint at index: {idx}")
        record = load_json_type_safe(filepath=file, return_type="dict")
        collision_ops.append(create_load_record_command(record=record, all_text=False))
        if len(collision_ops) == WRITE_BATCH:
            log_msg(logger=LOGGER, msg=f"Bulk writing at index: {idx + 1}.")
            bulk_load(
                dbh=dbh, ops=collision_ops, destination="collision", server=server
            )
            total_collision_ops += len(collision_ops)
            collision_ops = []
    if collision_ops:
        log_msg(logger=LOGGER, msg="Writing leftover records...")
        bulk_load(dbh=dbh, ops=collision_ops, destination="collision", server=server)
        total_collision_ops += len(collision_ops)
        collision_ops = []
    collision_elapsed_time = round(time.time() - collision_start_time, 2)
    log_msg(
        logger=LOGGER,
        msg=f"Finished loading collision data in {elapsed_time_formatter(collision_elapsed_time)}, completed {total_collision_ops} writes.",
    )

    log_msg(logger=LOGGER, msg="Calculating metadata stats...")
    stats_start_time = time.time()
    process_stats(
        dbh=dbh, data_collection=biomarker_collection, stat_collection=stats_collection
    )
    stats_elapsed_time = round(time.time() - stats_start_time, 2)
    log_msg(
        logger=LOGGER,
        msg=f"Finished calculating stats in {elapsed_time_formatter(stats_elapsed_time)}.",
    )

    clear_collection_elapsed_time = (
        clear_collection_elapsed_time
        if isinstance(clear_collection_elapsed_time, float)
        else 0.0
    )
    total_time = (
        canonical_id_load_time
        + second_id_load_time
        + clear_collection_elapsed_time
        + merged_elapsed_time
        + collision_elapsed_time
        + stats_elapsed_time
    )
    total_time_str = elapsed_time_formatter(total_time)
    finish_str = (
        "Finished loading data and calculating new metadata stats\n"
        f"{loading_coll_log}"
        f"\tClearing old data took {elapsed_time_formatter(clear_collection_elapsed_time)}\n"
        f"\tLoading merged data took {elapsed_time_formatter(merged_elapsed_time)}\n"
        f"\tLoading collision data took {elapsed_time_formatter(collision_elapsed_time)}\n"
        f"\tCalculating stats took {elapsed_time_formatter(stats_elapsed_time)}\n"
        f"\tTotal time: {total_time_str}"
    )
    log_msg(logger=LOGGER, msg=finish_str)

    return total_time_str


if __name__ == "__main__":
    parser, server_list = standard_parser()
    parser = notify_parser(parser=parser)
    parser.add_argument(
        "-c",
        "--continue",
        dest="continue_load",
        action="store_true",
        help="Continue from last successful index",
    )
    options = parser.parse_args()

    if not options.server:
        parser.print_help()
        sys.exit(1)
    server = options.server.lower().strip()
    if server not in server_list:
        print("Invalid server.")
        parser.print_help()
        sys.exit(1)
    if options.notify and not options.email:
        print("Notify was set to true but no emails were passed, see --help:\n")
        parser.print_help()
        sys.exit(1)

    subject: str
    message: str
    try:
        elapsed_time = main(options=options, server=server)
        subject = f"[SUCCESS] {server} Data Load Process Completed"
        message = f"Elapsed time: {elapsed_time}\n"
    except Exception as e:
        subject = f"[FAILED] {server} Data Load Process Completed"
        message = f"Data Load failed, check the logs.\n{e}\n{format_exc()}"
        log_msg(logger=LOGGER, msg=message, level="error")

    if options.notify:
        send_notification(
            email=options.email,
            subject=subject,
            message=message,
            server=server,
            logger=LOGGER,
        )
