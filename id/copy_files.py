import sys
import glob
import os
import subprocess

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.config import get_config
from tutils.parser import standard_parser, parse_server
from tutils.general import get_user_confirmation, copy_file


def main() -> None:

    parser, server_list = standard_parser()
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)
    if server != "dev":
        print("Can only run this script on the `dev` server.")
        sys.exit(1)

    config_obj = get_config()
    data_root_path = config_obj["data_path"]
    generated_path_segment = config_obj["generated_path_segment"]
    new_data_segment = config_obj["new_data_segment"]
    existing_data_segment = config_obj["existing_data_segment"]

    new_data_glob_pattern = os.path.join(
        data_root_path, *generated_path_segment, *new_data_segment, "*.json"
    )
    existing_data_path = os.path.join(
        data_root_path, *generated_path_segment, *existing_data_segment
    )

    files_to_copy = glob.glob(new_data_glob_pattern)
    confirmation_str = f"Going to copy the following files to {existing_data_path}:"
    for file in files_to_copy:
        confirmation_str += f"\n\t{file}"
    confirmation_str += f"To {existing_data_path}"

    rm_command = f"rm -r {existing_data_path}/*"
    confirmation_str += (
        f"\n\nWill clear out the existing data directory first with command: {rm_command}"
    )
    print(confirmation_str)
    get_user_confirmation()

    print("Cleaning existing data directory...")
    subprocess.run(rm_command, shell=True)
    print("Finished cleaning existing data directory")

    print("Copying files...")
    for fp in files_to_copy:
        print(f"Copying: `{fp}`...")
        copy_file(src=fp, dest=existing_data_path)


if __name__ == "__main__":
    main()
