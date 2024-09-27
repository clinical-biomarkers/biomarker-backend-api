"""Updates just the score and score_info fields in MongoDB.
"""

import argparse
from pymongo.collection import Collection
import sys
import os
import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.general import load_json_type_safe
from tutils.logging import setup_logging, log_msg
from tutils.parser import standard_parser, parse_server
from tutils.db import get_standard_db_handle
from tutils.config import get_config

LOGGER = setup_logging("update_scores.log")


def update_scores(file_list: list[str], collection_handle: Collection) -> bool:
    """Updates the scores in MongoDB.

    Parameters
    ----------
    file_list : list[str]
        The list of file paths to update in MongoDB.
    collection_handle : Collection
        The collection handle.

    Returns
    -------
    bool
        True on success, False on failure.
    """
    for fp in file_list:
        log_msg(logger=LOGGER, msg=f"Starting processing file: {fp}", to_stdout=True)
        data = load_json_type_safe(filepath=fp, return_type="list")
        for document in data:
            collision_status = document.pop("collision")
            if collision_status != 0:
                continue
            update_result = collection_handle.update_one(
                {"biomarker_id": document["biomarker_id"]},
                {
                    "$set": {
                        "score": document["score"],
                        "score_info": document["score_info"],
                    }
                },
            )
            if update_result.modified_count != 1:
                log_str = (
                    f"Error updating entry for biomarker ID {document['biomarker_id']}"
                )
                log_str += f"\nModified count: {update_result.modified_count}"
                log_str += f"\nEntry: {document}"
                log_msg(logger=LOGGER, msg=log_str, level="error", to_stdout=True)
                return False
        log_msg(
            logger=LOGGER,
            msg=f"Success: completed processing file: {fp}",
            to_stdout=True,
        )
    return True


def main():

    parser = argparse.ArgumentParser(prog="update_scores.py")
    parser, server_list = standard_parser()
    parser.add_argument(
        "glob_pattern", help="glob pattern for the files to update scores for"
    )
    if len(sys.argv) <= 2:
        sys.argv.append("-h")
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    glob_files = glob.glob(options.glob_pattern)
    if len(glob_files) == 0:
        log_msg(
            logger=LOGGER,
            msg="Glob pattern picked up zero files, check glob pattern.",
            level="error",
            to_stdout=True,
        )
        sys.exit(1)

    config_obj = get_config()
    db_name = config_obj["dbinfo"]["dbname"]
    biomarker_collection = config_obj["dbinfo"][db_name]["collection"]
    dbh = get_standard_db_handle(server=server)

    if update_scores(glob_files, dbh[biomarker_collection]):
        log_msg(logger=LOGGER, msg="Success!", to_stdout=True)
    else:
        log_msg(
            logger=LOGGER,
            msg="Failed somewhere, potential partial data load.",
            level="error",
            to_stdout=True,
        )


if __name__ == "__main__":
    main()
