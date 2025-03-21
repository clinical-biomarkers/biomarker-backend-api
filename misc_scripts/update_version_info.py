import sys
import os
import re
from datetime import datetime
import time
from pymongo.collection import Collection
from typing import Literal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.parser import standard_parser, parse_server
from tutils.constants import version_default
from tutils.db import get_standard_db_handle
from tutils.general import get_user_confirmation


def get_current_version(collection: Collection, component: Literal["api", "data"]):
    return collection.find_one({"component": component})


def is_valid_version(version: str, pattern: str) -> bool:
    """ Check if the version string matches the required pattern. """
    return bool(re.fullmatch(pattern, version))


def main() -> None:

    parser, server_list = standard_parser()
    parser.add_argument("--api-version", dest="api_version", type=str)
    parser.add_argument("--data-version", dest="data_version", type=str)

    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    api_version = options.api_version
    data_version = options.data_version

    api_pattern = r"^\d+\.\d+$" # Matches X.X (e.g. 1.0)
    data_pattern = r"^\d+\.\d+\.\d+$" # Matches X.X.X (e.g. 1.2.3)

    if not api_version and not data_version:
        print("Need to include one or both of api and data versions.")
        sys.exit(1)

    if api_version and not is_valid_version(api_version, api_pattern):
        print("Error: --api-version must be in the format X.X (e.g. 1.0)")
        sys.exit(1)

    if data_version and not is_valid_version(data_version, data_pattern):
        print("Error: --data-version must be in the format X.X.X (e.g. 1.2.3)")
        sys.exit(1)

    version_collection_name = version_default()

    current_time = datetime.now()
    time_string = current_time.strftime("%Y-%m-%d %H:%M:%S")
    timezone_name = time.strftime("%Z")
    timezone_offset = time.strftime("%z")
    formatted_time = f"{time_string} {timezone_name}{timezone_offset}"

    dbh = get_standard_db_handle(server=server)
    version_collection = dbh[version_collection_name]

    current_api_version = get_current_version(version_collection, "api")
    current_data_version = get_current_version(version_collection, "data")

    confirmation_msg = "Going to update:\n"

    if api_version:
        current_version = "None" if not current_api_version else current_api_version
        confirmation_msg += f"\n - API: {current_version} -> {api_version}"
    if data_version:
        current_version = "None" if not current_data_version else current_data_version
        confirmation_msg += f"\n - Data: {current_version} -> {data_version}"

    print(confirmation_msg)
    get_user_confirmation()

    if api_version:
        version_collection.update_one(
            {"component": "api"},
            {
                "$set": {
                    "component": "api",
                    "version": api_version,
                    "release_date": formatted_time,
                }
            },
            upsert=True,
        )
        print(f"Updated API version to {api_version}")

    if data_version:
        version_collection.update_one(
            {"component": "data"},
            {
                "$set": {
                    "component": "data",
                    "version": data_version,
                    "release_date": formatted_time,
                }
            },
            upsert=True,
        )
        print(f"Updated data version to {data_version}")


if __name__ == "__main__":
    main()
