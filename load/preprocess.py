"""Preprocesses the JSON data model data by attempting to merge collision records.

For now this is the implementation that I created on the fly. However, its slow, incurring significant runtime complexity.
As the project gets more and more data this approach will be very slow. This is a heavily IO bound workflow. In an ideal
world, you could complete this logic in memory and avoid excessive IO calls. However, with the amount of data we currently 
have that is not feasible.

usage: parser.py [-h] server

positional arguments:
  server      prd/beta/tst/dev

options:
  -h, --help  show this help message and exit
"""

import ijson
import time
import subprocess
import glob
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.general import (
    get_user_confirmation,
    load_json_type_safe,
    resolve_symlink,
    write_json,
)
from tutils.config import get_config
from tutils.logging import setup_logging, log_msg, start_message
from load.preprocess_utils import attempt_merge
from tutils.parser import standard_parser

LOGGER = setup_logging("preprocess_data.log")
CHECKPOINT_VAL = 5_000


def first_pass(files: list[str], merged_dir: str, collision_dir: str) -> float:
    """Handles the first pass through the files. Workflow is as follows:
        - The files found in the existing data directory (the files parameter) are looped through.
        - Each file is streamed using the ijson library.
        - If the collision value is 0, that specific record will be saved in a file in the merged directory with the filename {biomarker_id}.json.
        - If the collision value is 1, that record will be saved in a file in the collision_dir with the filename {biomarker_id}-{counter}.json.
        - If the collision value is 2, that record will be skipped.

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
    for file_idx, file_path in enumerate(files):
        log_msg(
            logger=LOGGER,
            msg="==================== Starting First Pass ====================",
            to_stdout=True,
        )
        log_msg(
            logger=LOGGER,
            msg=f"------------- Processing file {file_idx + 1} of {len(files)}: {os.path.basename(file_path)}",
            to_stdout=True,
        )
        file = open(file_path, "r")
        for record_idx, record in enumerate(ijson.items(file, "item")):
            total_record_count += 1
            if record_idx + 1 % CHECKPOINT_VAL == 0:
                print(f"Hit checkpoint at record index: {record_idx}.")

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
            elif collision == 2:
                continue
            else:
                raise ValueError(
                    f"Found invalid collision value: `{collision}` (idx: {record_idx})"
                )
        file.close()
    elapsed_time = round(time.time() - start_time)
    log_msg(
        logger=LOGGER,
        msg=f"Finished first pass, processed {total_record_count} records with {collision_count} collisions in {elapsed_time} seconds.",
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
        to_stdout=True,
    )
    all_collision_files = glob.glob(os.path.join(collision_dir, "*.json"))
    total_collision_files = len(all_collision_files)
    merged_count = 0

    start_time = time.time()
    for file_idx, file in enumerate(all_collision_files):
        if file_idx + 1 % CHECKPOINT_VAL == 0:
            print(f"Hit checkpoint at file index: {file_idx}.")

        collision_record = load_json_type_safe(filepath=file, return_type="dict")
        biomarker_id = collision_record["biomarker_id"]
        merge_record_path = os.path.join(merged_dir, f"{biomarker_id}.json")
        if not os.path.isfile(merge_record_path):
            raise ValueError(
                f"Did not find corresponding source record for collision biomarker id {biomarker_id}, file: {file}."
            )

        merge_record = load_json_type_safe(
            filepath=os.path.join(merged_dir, f"{biomarker_id}.json"),
            return_type="dict",
        )
        merge_result = attempt_merge(
            merge_record=merge_record, collision_record=collision_record
        )
        if merge_result is None:
            continue
        write_json(filepath=merge_record_path, data=merge_result)
        os.remove(file)
        merged_count += 1

    elapsed_time = round(time.time() - start_time, 2)
    log_msg(
        logger=LOGGER,
        msg=f"Finished second pass, was able to merge {merged_count} records out of {total_collision_files} in {elapsed_time} seconds.",
    )
    return elapsed_time


def main() -> None:

    parser, _ = standard_parser()
    options = parser.parse_args()
    if not options.server:
        parser.print_help()
        sys.exit(1)
    server = options.server.lower().strip()
    # only allow running this script on the tst server
    target_server = "tst"
    if server != target_server:
        print(f"This script can only be run on the {target_server} server.")
        sys.exit(1)

    start_message(logger=LOGGER, msg=f"Preprocessing data for server: {server}")

    config_obj = get_config()
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
    all_data_log_msg = "Found existing files:\n" + "\n\t".join(all_data_files)
    log_msg(logger=LOGGER, msg=all_data_log_msg, to_stdout=True)
    get_user_confirmation()

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
    if not os.path.isdir(merged_target_path_merged):
        os.mkdir(merged_target_path_merged)
    else:
        rm_command = ["rm", os.path.join(merged_target_path_merged, "*.json")]
        rm_command_str = " ".join(rm_command)
        print(
            f"Found existing directory at {merged_target_path_merged}, going to clear with the following command:\n\t{rm_command_str}"
        )
        get_user_confirmation()
        subprocess.run(rm_command)
    # create the path to the collision directory or clear them out if they exist
    # this is where the collision value != 0 records will go
    # after the first pass to dump the collision records here, each record will be attempted to be merged with the non-collision record
    # equivalent, if it cannot be, it will remain in this directory
    merged_target_path_collision = os.path.join(merged_target_path, "collision_json")
    if not os.path.isdir(merged_target_path_collision):
        os.mkdir(merged_target_path_collision)
    else:
        rm_command = ["rm", os.path.join(merged_target_path_collision, "*.json")]
        rm_command_str = " ".join(rm_command)
        print(
            f"Found existing directory at {merged_target_path_collision}, going to clear with the following command:\n\t{rm_command_str}"
        )
        get_user_confirmation()
        subprocess.run(rm_command)

    first_pass_time = first_pass(
        files=all_data_files,
        merged_dir=merged_target_path_merged,
        collision_dir=merged_target_path_collision,
    )
    second_pass_time = second_pass(
        merged_dir=merged_target_path_merged, collision_dir=merged_target_path_collision
    )
    finish_str = "Finished preprocessing data."
    finish_str += f"\n\tFirst pass took {first_pass_time} seconds."
    finish_str += f"\n\tSecond pass took {second_pass_time} seconds."
    finish_str += f"\n\tTotal time: {first_pass_time + second_pass_time} seconds."
    log_msg(logger=LOGGER, msg=finish_str, to_stdout=True)


if __name__ == "__main__":
    main()
