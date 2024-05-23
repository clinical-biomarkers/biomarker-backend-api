""" Checks a file for unique IDs.
"""

import argparse
import sys
from helpers import misc_functions as misc_fns


def main():

    parser = argparse.ArgumentParser(prog="check_unique_ids.py")
    parser.add_argument("file_path", help="filepath to the file to check")
    options = parser.parse_args()
    if len(sys.argv) <= 1:
        sys.argv.append("--help")
    file_path = options.file_path
    if not misc_fns.validate_filepath(filepath=file_path, mode="input"):
        print("Invalid filepath.")
        sys.exit(0)
    if not file_path.endswith(".json"):
        print("Invalid file extension, expects JSON.")
        sys.exit(0)
    data = misc_fns.load_json(file_path)
    if not isinstance(data, list):
        print(f"Error reading data, expected type `list`, got {type(data)}")
        sys.exit(0)

    seen_ids = set()
    for idx, document in enumerate(data):
        if document["biomarker_id"] not in seen_ids:
            seen_ids.add(document["biomarker_id"])
            continue
        print(f"Duplicate ID: {document['biomarker_id']}, index: {idx}")


if __name__ == "__main__":
    main()
