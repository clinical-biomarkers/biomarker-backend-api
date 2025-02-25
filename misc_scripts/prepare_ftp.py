import sys
import os
import glob
import subprocess
from time import time
from pprint import pformat

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils import ROOT_DIR
from tutils.config import get_config
from tutils.parser import standard_parser, parse_server
from tutils.logging import setup_logging, log_msg, start_message, elapsed_time_formatter
from tutils.general import (
    copy_file,
    resolve_symlink,
    get_user_confirmation,
    confirmation_message_complete,
)

ALL_BIOMARKER_JSON = "all-biomarker-json"
ALL_BIOMARKER_TSV = "all-biomarker-tsv"
ALL_BIOMARKER_JSON_MERGED = "all-biomarker-json-merged"

TAR_EXT = ".tar.gz"
TAR_CMD = "tar -czvf"

LOGGER = setup_logging("prepare_ftp.log")
SUBPROCESS_LOG_PATH = os.path.join(ROOT_DIR, "logs", f"prepare_ftp_subprocess.log")
CHECKPOINT = 1_000


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)
    if server != "prd":
        print("Can only run this script on the `prd` server.")
        sys.exit(1)

    start_message(logger=LOGGER, msg=f"Preparing FTP release for server: {server}")
    log_msg(
        logger=LOGGER,
        msg=f"Subprocess output will be logged to {SUBPROCESS_LOG_PATH}",
        to_stdout=True,
    )

    config_obj = get_config()
    data_root_path = config_obj["data_path"]
    generated_path_segment = config_obj["generated_path_segment"]
    existing_data_segment = config_obj["existing_data_segment"]
    merged_data_segment = config_obj["merged_data_segment"]
    ftp_path = config_obj["ftp_dir_path"]

    existing_data_dir = os.path.join(
        data_root_path, *generated_path_segment, *existing_data_segment
    )
    merged_data_dir = os.path.join(
        data_root_path, *generated_path_segment, *merged_data_segment
    )
    tsv_dir = "/data/shared/biomarkerdb/releases/data/current"

    data_config: dict[str, dict[str, str]] = {
        "json": {
            "resolved_symlink": "N/A",
            "src_glob_pattern": os.path.join(existing_data_dir, "*.json"),
            "dest_path": os.path.join(ftp_path, ALL_BIOMARKER_JSON),
            "tarball": os.path.join(ftp_path, f"{ALL_BIOMARKER_JSON}{TAR_EXT}"),
        },
        "tsv": {
            "resolved_symlink": resolve_symlink(tsv_dir) or f"UNABLE TO RESOLVE SYMLINK: {tsv_dir}",
            "src_glob_pattern": os.path.join(tsv_dir, "reviewed", "*.tsv"),
            "dest_path": os.path.join(ftp_path, ALL_BIOMARKER_TSV),
            "tarball": os.path.join(ftp_path, f"{ALL_BIOMARKER_TSV}{TAR_EXT}"),
        },
        "merged": {
            "resolved_symlink": resolve_symlink(merged_data_dir)
            or f"UNABLE TO RESOLVE SYMLINK: {merged_data_dir}",
            "src_glob_pattern": os.path.join(merged_data_dir, "merged_json", "*.json"),
            "dest_path": os.path.join(ftp_path, ALL_BIOMARKER_JSON_MERGED),
            "tarball": os.path.join(ftp_path, f"{ALL_BIOMARKER_JSON_MERGED}{TAR_EXT}"),
        },
    }
    log_msg(logger=LOGGER, msg=f"\n{pformat(data_config)}")

    confirmation_str = "Confirmation data:"
    for idx, (data_type, metadata) in enumerate(data_config.items()):
        confirmation_str += f"\n\t{idx}. {data_type}"
        confirmation_str += f"\n\t\tResolved symlink: {metadata['resolved_symlink']}"
        confirmation_str += f"\n\t\tSource glob pattern: {metadata['src_glob_pattern']}"
        confirmation_str += f"\n\t\tDestination: {metadata['dest_path']}"
        confirmation_str += f"\n\t\tTarball: {metadata['tarball']}"
    print(confirmation_str)
    get_user_confirmation()
    confirmation_message_complete()

    start_time = time()

    f = open(SUBPROCESS_LOG_PATH, "w")
    checkpoint = CHECKPOINT
    try:
        for data_type, metadata in data_config.items():
            data_type_start_time = time()
            start_str = ("-" * 50) + f"Processing {data_type} data" + ("-" * 50)
            log_msg(logger=LOGGER, msg=start_str)
            log_msg(logger=LOGGER, msg=pformat(metadata))

            if not os.path.isdir(metadata["dest_path"]):
                os.mkdir(metadata["dest_path"])

            src_glob_pattern = metadata["src_glob_pattern"]
            files_to_copy = glob.glob(src_glob_pattern)
            log_msg(logger=LOGGER, msg=f"Copying from: {src_glob_pattern}")
            copy_start_time = time()
            for idx, fp in enumerate(files_to_copy):
                if (idx + 1) % checkpoint == 0:
                    log_msg(logger=LOGGER, msg=f"Hit checkpoint at idx: {idx + 1}")
                copy_file(src=fp, dest=metadata["dest_path"])
            log_msg(
                logger=LOGGER,
                msg=f"Copying took {elapsed_time_formatter(time() - copy_start_time)}",
            )

            tar_cmd = f"{TAR_CMD} {metadata['tarball']} -C {metadata['dest_path']}"
            log_msg(logger=LOGGER, msg=f"Running tar command:\n\t{tar_cmd}")
            tar_start_time = time()
            subprocess.run(tar_cmd, shell=True, stdout=f, stderr=subprocess.STDOUT)
            log_msg(
                logger=LOGGER,
                msg=f"Tar command took: {elapsed_time_formatter(time() - tar_start_time)}",
            )

            if data_type == "merged":
                log_msg(logger=LOGGER, msg="Removing individual merged data files...")
                subprocess.run(f"rm -r {metadata['dest_path']}", shell=True, stdout=f)

            log_msg(
                logger=LOGGER,
                msg=f"Full {data_type} process took {elapsed_time_formatter(time() - data_type_start_time)}",
            )

        f.close()
    except Exception as e:
        f.close()
        elapsed_time = time() - start_time
        log_msg(
            logger=LOGGER,
            msg=f"Failed in {elapsed_time_formatter(elapsed_time)} with: {e}",
            level="error",
        )

    log_msg(
        logger=LOGGER, msg=f"Finished in {elapsed_time_formatter(time() - start_time)}"
    )


if __name__ == "__main__":
    main()
