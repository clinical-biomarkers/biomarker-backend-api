import ijson
import time
import subprocess
import glob
import os
import sys
from traceback import format_exc

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.general import (
    get_user_confirmation,
    load_json_type_safe,
    resolve_symlink,
    write_json,
    confirmation_message_complete,
)
from tutils.config import get_config
from tutils.logging import setup_logging, log_msg, start_message, elapsed_time_formatter
from load.preprocess_utils import attempt_merge
from tutils.parser import standard_parser, notify_parser
from tutils.notify import send_notification

LOGGER = setup_logging("preprocess_data.log")
CHECKPOINT_VAL = 5_000


def first_pass(files: list[str], merged_dir: str, collision_dir: str) -> float:
    """Handles the first pass through the files. Workflow is as follows:
        - The files found in the existing data directory (the files parameter) are looped through.
        - Each file is streamed using the ijson library.
        - If the collision value is 0, that specific record will be saved in a file in the merged directory with the filename {biomarker_id}.json.
        - If the collision value is 1, that record will be saved in a file in the collision_dir with the filename {biomarker_id}-{counter}.json.

    Parameters
    ----------
    files: list[str]
        The files found through the glob pattern over the existing data.
    merged_dir: str
        Path to the merged directory.
    collision_dir: str
        Path to the collision directory.

    Returns
    -------
    float
        Seconds elapsed.
    """

    def get_next_available_filename(base_filename: str, directory: str) -> str:
        """Generates the next available filename by incrementing the counter at the end of the filename."""
        counter = 0
        filepath = os.path.join(directory, f"{base_filename}-{counter}.json")
        while os.path.isfile(filepath):
            counter += 1
            filepath = os.path.join(directory, f"{base_filename}-{counter}.json")
        return filepath

    start_time = time.time()
    total_record_count = 0
    collision_count = 0
    log_msg(
        logger=LOGGER,
        msg="==================== Starting First Pass ====================",
    )
    for file_idx, file_path in enumerate(files):
        log_msg(
            logger=LOGGER,
            msg=f"------------- Processing file {file_idx + 1} of {len(files)}: {os.path.basename(file_path)}",
        )
        file = open(file_path, "r")
        file_start_time = time.time()
        record_counter = 0
        for record_idx, record in enumerate(ijson.items(file, "item")):
            total_record_count += 1
            if (record_idx + 1) % CHECKPOINT_VAL == 0:
                log_msg(
                    logger=LOGGER, msg=f"Hit checkpoint at record index: {record_idx}"
                )

            collision = record.pop("collision")
            biomarker_id = record["biomarker_id"]
            if "score" in record:
                _ = record.pop("score")
            if "score_info" in record:
                _ = record.pop("score_info")

            if collision == 0:
                output_path = os.path.join(merged_dir, f"{biomarker_id}.json")
                if os.path.isfile(output_path):
                    raise ValueError(
                        f"File for {output_path} already exists (idx: {record_idx})."
                    )
                write_json(filepath=output_path, data=record, include_default=True)
            elif collision == 1:
                collision_count += 1
                output_path = get_next_available_filename(biomarker_id, collision_dir)
                write_json(filepath=output_path, data=record, include_default=True)
            else:
                raise ValueError(
                    f"Found invalid collision value: `{collision}` (idx: {record_idx})"
                )
            record_counter += 1
        file.close()
        msg = (
            f"Elapsed time: {elapsed_time_formatter(time.time() - file_start_time)}\n"
            f"Records processed: {record_counter}"
        )
        log_msg(logger=LOGGER, msg=msg)
    elapsed_time = round(time.time() - start_time)
    log_msg(
        logger=LOGGER,
        msg=f"Finished first pass, processed {total_record_count} records with {collision_count} collisions, elapsed time: {elapsed_time_formatter(elapsed_time)}",
    )
    return elapsed_time


def second_pass(merged_dir: str, collision_dir: str) -> float:
    """Handles the second pass through the data where the collision records are attempted to be merged into the
    non-collision records. Workflow is as follows:
        - Glob all the JSON files in the collision directory.
        - Compare to existing record in the merged_dir.
        - Check if it can be automatically merged.
        - If it can be merged, merge it into the existing record and delete the record in the collision directory.
          - If merged, will have to re-update any other collision filename counters.
        - If it can't be merged, leave it in the collision directory.

    Parameters
    ----------
    merged_dir: str
        Path to the merged directory.
    collision_dir: str
        Path to the collision directory.

    Returns
    -------
    float
        Seconds elapsed.
    """
    log_msg(
        logger=LOGGER,
        msg="==================== Starting Second Pass ====================",
    )
    all_collision_files = glob.glob(os.path.join(collision_dir, "*.json"))
    collision_file_sort_start = time.time()
    all_collision_files.sort()
    collision_file_sort_elapsed = time.time() - collision_file_sort_start
    log_msg(
        logger=LOGGER,
        msg=f"Collision file sort took {elapsed_time_formatter(collision_file_sort_elapsed)}",
    )
    total_collision_files = len(all_collision_files)
    merged_count = 0
    merged_record_ids = []

    start_time = time.time()
    for file_idx, file in enumerate(all_collision_files):

        if (file_idx + 1) % CHECKPOINT_VAL == 0:
            log_msg(logger=LOGGER, msg=f"Hit checkpoint at file index: {file_idx}")

        collision_record = load_json_type_safe(filepath=file, return_type="dict")
        biomarker_id = collision_record["biomarker_id"]
        merge_record_path = os.path.join(merged_dir, f"{biomarker_id}.json")
        if not os.path.isfile(merge_record_path):
            write_json(filepath=merge_record_path, data=collision_record)
            os.remove(file)
            continue
        merge_record = load_json_type_safe(
            filepath=merge_record_path,
            return_type="dict",
        )
        merge_result = attempt_merge(
            merge_record=merge_record, collision_record=collision_record
        )
        if merge_result is None:
            log_msg(
                logger=LOGGER,
                msg=f"Unable to merge:\n\t- {file}\n\t- {merge_record_path}\nContinuing...",
            )
            continue
        write_json(filepath=merge_record_path, data=merge_result)
        os.remove(file)
        merged_count += 1
        merged_record_ids.append(merge_result["biomarker_id"])

    elapsed_time = round(time.time() - start_time, 2)
    msg = (
        f"Finished second pass, was able to merge {merged_count} records out of {total_collision_files} in {elapsed_time_formatter(elapsed_time)}.\n"
        "Merged record IDs: " + ", ".join(merged_record_ids)
    )
    log_msg(
        logger=LOGGER,
        msg=msg,
    )
    return elapsed_time


def main(server: str) -> str:

    start_message(logger=LOGGER, msg=f"Preprocessing data for server: {server}")

    config_obj = get_config(logger=LOGGER)
    # the root path to the biomarkerdb data
    data_root_path_segment = config_obj["data_path"]
    # the path elements to the generated datamodel data from the root path segment
    generated_path_segment = config_obj["generated_path_segment"]
    # the path elements to the existing datamodel data (latest version of the existing data) from the generated path segment
    existing_data_path_segment = config_obj["existing_data_segment"]
    # the path elements to the merged datamodel data from the generated path segment
    merged_data_path_segment = config_obj["merged_data_segment"]

    existing_data_pattern = os.path.join(
        data_root_path_segment,
        *generated_path_segment,
        *existing_data_path_segment,
        "*.json",
    )
    # grab all the files in the existing data directory (latest version of each JSON datamodel formatted data)
    all_data_files = glob.glob(existing_data_pattern)
    all_files_sort_start = time.time()
    all_data_files.sort()
    all_file_sort_elapsed = time.time() - all_files_sort_start
    all_data_log_msg = "Found existing files:\n\t" + "\n\t".join(all_data_files)
    all_data_log_msg += (
        f"\nAll files sort took  {elapsed_time_formatter(all_file_sort_elapsed)}"
    )
    log_msg(logger=LOGGER, msg=all_data_log_msg, to_stdout=True)
    get_user_confirmation()

    clear_cmds = []
    # create the path to the merged data directory
    merged_target_path = os.path.join(
        data_root_path_segment, *generated_path_segment, *merged_data_path_segment
    )
    resolved_symlink = resolve_symlink(merged_target_path)
    print(f"Resolved symlink for {merged_target_path} points to:\n\t{resolved_symlink}")
    get_user_confirmation()
    # create the path to the merged json directory or clear them out if they exist
    # this is where the finalized merged JSON data will go
    merged_target_path_merged = os.path.join(merged_target_path, "merged_json")
    if os.path.isdir(merged_target_path_merged):
        rm_command = f"rm -r {merged_target_path_merged}"
        clear_cmds.append(rm_command)
        confirmation_str = f"Found existing directory at {merged_target_path_merged}, going to remove with the following command:"
        confirmation_str += f"\n\t{rm_command}"
        print(confirmation_str)
        get_user_confirmation()

    # create the path to the collision directory or clear them out if they exist
    # this is where the collision value != 0 records will go
    # after the first pass to dump the collision records here, each record will be attempted to be merged with the non-collision record
    # equivalent, if it cannot be, it will remain in this directory
    merged_target_path_collision = os.path.join(merged_target_path, "collision_json")
    if os.path.isdir(merged_target_path_collision):
        rm_command = f"rm -r {merged_target_path_collision}"
        clear_cmds.append(rm_command)
        confirmation_str = f"Found existing directory at {merged_target_path_collision}, going to clear with the following command:"
        confirmation_str += f"\n\t{rm_command}"
        print(confirmation_str)
        get_user_confirmation()

    confirmation_message_complete()

    for cmd in clear_cmds:
        log_msg(logger=LOGGER, msg=f"Running command: {cmd}...")
        rm_time = time.time()
        subprocess.run(cmd, shell=True)
        rm_elapsed = time.time() - rm_time
        log_msg(
            logger=LOGGER,
            msg=f"Command: {cmd} took {elapsed_time_formatter(rm_elapsed)}.",
        )
    os.mkdir(merged_target_path_merged)
    os.mkdir(merged_target_path_collision)

    first_pass_time = first_pass(
        files=all_data_files,
        merged_dir=merged_target_path_merged,
        collision_dir=merged_target_path_collision,
    )
    second_pass_time = second_pass(
        merged_dir=merged_target_path_merged, collision_dir=merged_target_path_collision
    )
    finish_str = "Finished preprocessing data."
    finish_str += f"\n\tFirst pass took {elapsed_time_formatter(first_pass_time)}."
    finish_str += f"\n\tSecond pass took {elapsed_time_formatter(second_pass_time)}."
    total_elapsed_time_str = elapsed_time_formatter(first_pass_time + second_pass_time)
    finish_str += f"\n\tTotal time: {total_elapsed_time_str}."
    log_msg(logger=LOGGER, msg=finish_str)

    return total_elapsed_time_str


if __name__ == "__main__":
    parser, _ = standard_parser()
    parser = notify_parser(parser=parser)
    options = parser.parse_args()

    if not options.server:
        parser.print_help()
        sys.exit(1)
    server = options.server.lower().strip()
    # only allow running this script on the dev server
    if server != "dev":
        print("This script can only be run on the dev server.")
        sys.exit(1)
    if options.notify and not options.email:
        print("Notify was set to true but no emails were passed, see --help:\n")
        parser.print_help()
        sys.exit(1)

    try:
        elapsed_time = main(server=server)
        subject = f"[SUCCESS] {server} Preprocessing Completed"
        message = f"Elapsed time: {elapsed_time}"
    except Exception as e:
        subject = f"[FAILED] {server} Preprocessing Completed"
        message = f"Preprocess failed: {e}\n{format_exc()}"
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
