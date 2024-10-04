import sys
import os
import glob
import subprocess

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.config import get_config
from tutils.parser import standard_parser, parse_server
from tutils.general import copy_file, resolve_symlink, get_user_confirmation


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

    existing_data_resolved_symlink = resolve_symlink(existing_data_dir)
    merged_data_resolved_symlink = resolve_symlink(merged_data_dir)
    tsv_data_resolved_symlink = resolve_symlink(tsv_dir)

    existing_data_glob_pattern = os.path.join(existing_data_dir, "*.json")
    merged_data_glob_pattern = os.path.join(merged_data_dir, "merged_json", "*.json")
    tsv_glob_pattern = os.path.join(tsv_dir, "reviewed", "*")

    existing_data_dest_path = os.path.join(ftp_path, "all-biomarker-json")
    merged_data_dest_path = os.path.join(ftp_path, "all-biomarker-json-merged")
    tsv_dest_path = os.path.join(ftp_path, "all-biomarker-tsv")

    confirmation_str = "Copying files from:"
    confirmation_str += f"\n\t1. {existing_data_glob_pattern}"
    confirmation_str += f"\n\t\t Resolved symlink: {existing_data_resolved_symlink}"
    confirmation_str += f"\n\t2. {merged_data_glob_pattern}"
    confirmation_str += f"\n\t\t Resolved symlink: {merged_data_resolved_symlink}"
    confirmation_str += f"\n\t3. {tsv_glob_pattern}"
    confirmation_str += f"\n\t\t Resolved symlink: {tsv_data_resolved_symlink}"
    confirmation_str += "Copying to:"
    confirmation_str += f"\n\t1. {existing_data_dest_path}"
    confirmation_str += f"\n\t2. {merged_data_dest_path}"
    confirmation_str += f"\n\t3. {tsv_dest_path}"
    print(confirmation_str)
    get_user_confirmation()

    if not os.path.isdir(existing_data_dest_path):
        os.mkdir(existing_data_dest_path)
    existing_data_files_to_copy = glob.glob(existing_data_glob_pattern)
    for fp in existing_data_files_to_copy:
        copy_file(src=fp, dest=existing_data_dest_path)

    if not os.path.isdir(merged_data_dest_path):
        os.mkdir(merged_data_dest_path)
    merged_data_files_to_copy = glob.glob(merged_data_glob_pattern)
    for fp in merged_data_files_to_copy:
        copy_file(src=fp, dest=merged_data_dest_path)

    if not os.path.isdir(tsv_dest_path):
        os.mkdir(tsv_dest_path)
    tsv_files_to_copy = glob.glob(tsv_glob_pattern)
    for fp in tsv_files_to_copy:
        copy_file(src=fp, dest=tsv_dest_path)

    # TODO : Create tarballs

if __name__ == "__main__":
    main()
