"""Updates just the score and score_info fields in MongoDB.
"""

import argparse
import json
import pymongo
from pymongo.database import Database
import sys
import logging
import glob
from map_scores import _handle_output

host = "mongodb://127.0.0.1:"
tst_port = "6061"
prd_port = "7071"
db_name = "biomarkerdb_api"
db_user = "biomarkeradmin"
db_pass = "biomarkerpass"
auth_mechanism = "SCRAM-SHA-1"
biomarker_collection = "biomarker_collection"


def update_scores(file_list: list[str], dbh: Database) -> bool:
    """Updates the scores in MongoDB.

    Parameters
    ----------
    file_list : list[str]
        The list of file paths to update in MongoDB.
    dbh : Database
        The database handle.

    Returns
    -------
    bool
        True on success, False on failure.
    """
    collection_handle = dbh[biomarker_collection]
    for fp in file_list:
        _handle_output("info", f"Starting processing file: {fp}")
        data = json.load(open(fp, "r"))
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
                _handle_output(
                    "error",
                    f"Error updating entry for biomarker ID {document['biomarker_id']}\nModified count: {update_result.modified_count}\nEntry: {document}.",
                )
                return False
        _handle_output("info", f"Success: completed processing file: {fp}")
    return True


def main():

    parser = argparse.ArgumentParser(prog="update_scores.py")
    parser.add_argument("server", help="tst/prd")
    parser.add_argument(
        "glob_pattern", help="glob pattern for the files to update scores for"
    )
    if len(sys.argv) <= 2:
        sys.argv.append("-h")
    options = parser.parse_args()
    server = options.server.lower().strip()
    if server not in {"tst", "prd"}:
        print("Invalid server.")
        sys.exit(1)

    logging.basicConfig(
        filename="update_score.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    glob_files = glob.glob(options.glob_pattern)
    if len(glob_files) == 0:
        _handle_output(
            "error", "Glob pattern picked up zero files, check glob pattern."
        )
        sys.exit(1)

    host_w_port = f"{host}{tst_port}" if server == "tst" else f"{host}{prd_port}"

    try:
        client = pymongo.MongoClient(
            host_w_port,
            username=db_user,
            password=db_pass,
            authSource=db_name,
            authMechanism=auth_mechanism,
            serverSelectionTimeoutMS=1000,
        )
        client.server_info()
        dbh = client[db_name]
    except Exception as e:
        print(e)
        sys.exit(1)

    if update_scores(glob_files, dbh):
        _handle_output("info", "Success!")
    else:
        _handle_output("error", "Failed somewhere, potential partial data load.")


if __name__ == "__main__":
    main()
