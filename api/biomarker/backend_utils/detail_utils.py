""" Handles the backend logic for the biomarker detail endpoints.
"""

from flask import Request, request
from flask import current_app
from typing import Tuple
from . import DB_COLLECTION
from . import db as db_utils
from . import utils as utils

def detail(api_request: Request, biomarker_id: str) -> Tuple[int, dict]:
    """Entry point for the backend logic of the detail endpoint, which
    takes a biomarker ID and returns the full JSON data model.

    Parameters
    ----------
    request : Request
        The flask request object.
    biomarker_id : str
        The biomarker id passed by the route.

    Returns
    -------
    tuple : (int, dict)
        The HTTP code and return JSON.
    """
    # build the base request object for the user request
    request_object = {"biomarker_id": biomarker_id}
    # get the additional payload or query string arguments if provided
    # TODO : get_request_object will handle validation
    arguments = utils.get_request_object(api_request, "detail")

