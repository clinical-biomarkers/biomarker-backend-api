import argparse
import sys
import glob
from helpers import misc_functions as misc_fns


def main():

    ### handle command line arguments
    parser = argparse.ArgumentParser(
        prog="copy_files.py", usage="python copy_files.py server"
    )
    parser.add_argument("-s", "--server", help="tst")
    options = parser.parse_args()
    if not options.server:
        parser.print_help()
        sys.exit(1)
    server = options.server
    if server.lower() == "prd":
        print("Cannot run this script on prd server.")
        sys.exit(1)
    if server.lower() not in {"tst"}:
        print("Invalid server name.")
        sys.exit(1)

    ### get config info
    config_obj = misc_fns.load_json("../api/config.json")
    if not isinstance(config_obj, dict):
        print("Invalid config object type, expected dict.")
        sys.exit(1)
    data_root_path = config_obj["data_path"]

    ### copy files
    data_release_glob_pattern = (
        f"{data_root_path}/generated/datamodel/new_data/current/*.json"
    )
    existing_data_path = f"{data_root_path}/generated/datamodel/existing_data"
    for fp in glob.glob(data_release_glob_pattern):
        if "load_map.json" in fp:
            continue
        if misc_fns.copy_file(fp, existing_data_path):
            print(f"Successfully copied file: {fp}.")
        else:
            print(f"FAILED to copy file: {fp}.")


if __name__ == "__main__":
    main()
