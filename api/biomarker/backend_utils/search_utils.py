""" Handles the backend logic for the biomarker search endpoints.
"""

from flask import Request
from typing import Tuple, Dict

from . import db as db_utils
from . import utils as utils


def init() -> Tuple[Dict, int]:
    """Gets the searchable fields? Not really sure the purpose
    of this endpoint, copying Robel's response object.

    Returns
    -------
    tuple : (dict, int)
        The searchable fields and the HTTP code.
    """
    # TODO : implement a separate collection for this eventually, hardcoding for now
    response_object = {
        "best_biomarker_role": [
            "prognostic",
            "diagnostic",
            "monitoring",
            "risk",
            "predictive",
            "safety",
            "response",
        ],
        "assessed_entity_type": [
            "protein",
            "glycan",
            "metabolite",
            "gene",
            "chemical element",
            "cell",
        ],
        "simple_search_category": [
            {"id": "any", "display": "Any"},
            {"id": "biomarker", "display": "Biomarker"},
            {"id": "condition", "display": "Condition"},
        ],
    }
    return response_object, 200

def simple_search(api_request: Request) -> Tuple[Dict, int]:
    """Entry point for the backend logic of the search/simple endpoint.

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (int, dict)
        The return JSON and HTTP code.
    """
    arguments = utils.get_request_object(api_request, "simple_search")
