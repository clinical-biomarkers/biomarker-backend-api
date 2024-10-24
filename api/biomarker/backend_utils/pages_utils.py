""" Handles the backend logic for the pages endpoints.
"""

from typing import Tuple, Dict

from . import db as db_utils
from . import utils as utils


def home_init() -> Tuple[Dict, int]:
    """Entry point for the backend logic of the home init endpoint.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    stats, stats_http_code = db_utils.get_stats(mode="both")
    if stats_http_code != 200:
        return stats, stats_http_code

    return_object = {"statistics": stats}

    return return_object, 200
