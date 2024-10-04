""" Checks a file for unique IDs.
"""

import argparse
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.general import load_json_type_safe


def main():

    parser = argparse.ArgumentParser(prog="check_unique_ids.py")
    parser.add_argument("file_path", help="filepath to the file to check")
    options = parser.parse_args()
    if len(sys.argv) <= 1:
        parser.print_help()
        sys.exit(1)
    file_path = options.file_path
    if not file_path.endswith(".json"):
        print("Invalid file extension, expects JSON.")
        sys.exit(1)
    data = load_json_type_safe(filepath=file_path, return_type="list")

    seen_ids = set()
    for idx, document in enumerate(data):
        if document["biomarker_id"] not in seen_ids:
            seen_ids.add(document["biomarker_id"])
            continue
        print(f"Duplicate ID: {document['biomarker_id']}, index: {idx}")


if __name__ == "__main__":
    main()
