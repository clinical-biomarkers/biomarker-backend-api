import sys
import os
import glob
import subprocess

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.config import get_config
from tutils.parser import standard_parser, parse_server
from tutils.general import copy_file, resolve_symlink, get_user_confirmation, confirmation_message_complete

ALL_BIOMARKER_JSON = "all-biomarker-json"
ALL_BIOMARKER_TSV = "all-biomarker-tsv"
ALL_BIOMARKER_JSON_MERGED = "all-biomarker-json-merged"

TAR_EXT = ".tar.gz"
TAR_CMD = "tar -czvf"


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)
    if server != "prd":
        print("Can only run this script on the `prd` server.")
        sys.exit(1)

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
            "resolved_symlink": resolve_symlink(tsv_dir) or "UNABLE TO RESOLVE SYMLINK",
            "src_glob_pattern": os.path.join(tsv_dir, "reviewed", "*.tsv"),
            "dest_path": os.path.join(ftp_path, ALL_BIOMARKER_TSV),
            "tarball": os.path.join(ftp_path, f"{ALL_BIOMARKER_TSV}{TAR_EXT}"),
        },
        "merged": {
            "resolved_symlink": resolve_symlink(merged_data_dir)
            or "UNABLE TO RESOLVE SYMLINK",
            "src_glob_pattern": os.path.join(merged_data_dir, "merged_json", "*.json"),
            "dest_path": os.path.join(ftp_path, ALL_BIOMARKER_JSON_MERGED),
            "tarball": os.path.join(ftp_path, f"{ALL_BIOMARKER_JSON_MERGED}{TAR_EXT}"),
        },
    }

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

    for data_type, metadata in data_config.items():
        if not os.path.isdir(metadata["dest_path"]):
            os.mkdir(metadata["dest_path"])
        files_to_copy = glob.glob(metadata["src_glob_pattern"])
        for fp in files_to_copy:
            copy_file(src=fp, dest=metadata["dest_path"])
        subprocess.run(
            f"{TAR_CMD} {metadata['tarball']} -C {metadata['dest_path']}", shell=True
        )
        if data_type == "merged":
            subprocess.run(f"rm -r {metadata['dest_path']}", shell=True)


if __name__ == "__main__":
    main()
