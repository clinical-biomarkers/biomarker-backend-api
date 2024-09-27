"""Maps score values from the biomarker score calculator command line tool to the data model JSON
files.

Usage: map_scores.py [options]

    Positional arguments:
        glob_pattern    glob pattern of the JSON data model files to map scores to
        score_map       filepath to the score map to map from
"""

import argparse
import json
import glob
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.general import load_json_type_safe
from tutils.logging import setup_logging, log_msg

LOGGER = setup_logging("map_scores.log")


def update_biomarker_files(glob_pattern: str, score_map_path: str):
    """Maps the scores from the score map to the JSON data model files.

    Parameters
    ----------
    glob_pattern : str
        The glob pattern to the data files to update.
    score_map_path : str
        The path to the score map file.
    """
    # make sure glob pattern is valid
    processed_files: set[str] = set()
    glob_files = set([os.path.basename(file) for file in glob.glob(glob_pattern)])
    if not glob_files:
        log_msg(
            logger=LOGGER,
            msg="Error: glob pattern picked up zero files. Check glob pattern.",
            level="error",
            to_stdout=True,
        )
        sys.exit(1)

    # load score map
    score_map = load_json_type_safe(filepath=score_map_path, return_type="dict")
    score_map_files = set(score_map.keys())

    if glob_files != score_map_files:
        log_str = "Warning: glob files picked up do not match all keys listed in score map keys."
        log_str += f"\nglob files: {glob_files}"
        log_str += f"\nscore map files: {score_map_files}"
        log_msg(
            logger=LOGGER,
            msg=log_str,
            level="warning",
            to_stdout=True,
        )

    # work through data files
    for fp in glob.glob(glob_pattern):

        filename = os.path.basename(fp)
        if filename in processed_files:
            log_msg(
                logger=LOGGER,
                msg=f"Duplicate file found for file name: {filename}. Skipping duplicate...",
                level="warning",
                to_stdout=True,
            )
            continue
        processed_files.add(filename)

        if filename not in score_map:
            log_msg(
                logger=LOGGER,
                msg=f"Error: file {filename} not in score map, skipping...",
                level="error",
                to_stdout=True,
            )
            continue
        scores = score_map[filename]

        biomarker_data = load_json_type_safe(filepath=fp, return_type="list")
        for idx, biomarker in enumerate(biomarker_data):
            biomarker_id = biomarker.get("biomarker_id", None)
            if biomarker_id is None:
                log_msg(
                    logger=LOGGER,
                    msg=f"Error on index {idx} of file {filename}. No biomarker_id found. Skipping... \nEntry: {biomarker}",
                    level="error",
                    to_stdout=True,
                )
                continue
            if biomarker_id not in scores:
                log_msg(
                    logger=LOGGER,
                    msg=f"Biomarker ID {biomarker_id}, index: {idx} in file {filename} not found in score map. skipping...",
                    level="error",
                    to_stdout=True,
                )
                continue
            biomarker["score"] = scores[biomarker_id]["score"]
            biomarker["score_info"] = scores[biomarker_id]["score_info"]

        with open(fp, "w") as outfile:
            json.dump(biomarker_data, outfile, indent=4)
        log_msg(
            logger=LOGGER, msg=f"Successfully mapped file {filename}.", to_stdout=True
        )


def main():

    parser = argparse.ArgumentParser(
        prog="map_scores.py",
    )
    parser.add_argument(
        "glob_pattern", help="glob pattern for the files to map scores for"
    )
    parser.add_argument("score_map", help="filepath to the score map to map from")
    if len(sys.argv) <= 2:
        sys.argv.append("-h")
    options = parser.parse_args()

    update_biomarker_files(options.glob_pattern, options.score_map)


if __name__ == "__main__":
    main()
