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
import logging
from typing import Union


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
        _handle_output(
            "error", "Error: glob pattern picked up zero files. Check glob pattern."
        )
        sys.exit(1)

    # load score map
    score_map = _load_json(score_map_path)
    if not isinstance(score_map, dict):
        _handle_output(
            "error",
            f"Error reading score_map, expected dict and got {type(score_map)}.",
        )
        sys.exit(1)
    score_map_files = set(score_map.keys())

    if glob_files != score_map_files:
        _handle_output(
            "warning",
            f"Warning: glob files picked up do not match all keys listed in score map keys.\nglob files: {glob_files}\nscore map files: {score_map_files}",
        )

    # work through data files
    for fp in glob.glob(glob_pattern):

        filename = os.path.basename(fp)
        if filename in processed_files:
            _handle_output(
                "warning",
                f"Duplicate file found for file name: {filename}. Skipping duplicate...",
            )
            continue
        processed_files.add(filename)

        if filename not in score_map:
            _handle_output(
                "error", f"Error: file {filename} not in score map, skipping..."
            )
            continue
        scores = score_map[filename]

        biomarker_data = _load_json(fp)
        if not isinstance(biomarker_data, list):
            _handle_output(
                "error",
                f"Error reading file {filename}, expected list and got {type(biomarker_data)}. Skipping...",
            )
            continue
        for idx, biomarker in enumerate(biomarker_data):
            biomarker_id = biomarker.get("biomarker_id", None)
            if biomarker_id is None:
                _handle_output(
                    "error",
                    f"Error on index {idx} of file {filename}. No biomarker_id found. Skipping... \nEntry: {biomarker}",
                )
                continue
            if biomarker_id not in scores:
                _handle_output(
                    "error",
                    f"Biomarker ID {biomarker_id}, index: {idx} in file {filename} not found in score map. skipping...",
                )
                continue
            biomarker["score"] = scores[biomarker_id]["score"]
            biomarker["score_info"] = scores[biomarker_id]["score_info"]

        with open(fp, "w") as outfile:
            json.dump(biomarker_data, outfile, indent=4)
        _handle_output("info", f"Successfully mapped file {filename}.")


def _handle_output(level: str, message: str) -> None:
    """Handles print and logging.

    Parameters
    ----------
    level : str
        Logging level (accepts "info", "warning", "error")
    message : str
        The message to print and log.
    """
    if level == "info":
        logging.info(message)
        print(message)
    elif level == "warning":
        logging.warning(message)
        print(message)
    elif level == "error":
        logging.error(message)
        print(message)
    else:
        logging.error(f"Unsupported logging level. Message: {message}")


def _load_json(fp: str) -> Union[dict, list]:
    """Reads in a JSON file.

    Parameters
    ----------
    fp : str
        The filepath to the file.

    Returns
    -------
    dict or list
        The JSON data.
    """
    try:
        with open(fp, "r") as f:
            json_obj = json.load(f)
    except FileNotFoundError as e:
        print(f"FileNotFoundError in load_json for filepath: `{fp}`.\n{e}")
        sys.exit(1)
    return json_obj


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

    logging.basicConfig(
        filename="map_score.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    update_biomarker_files(options.glob_pattern, options.score_map)


if __name__ == "__main__":
    main()
