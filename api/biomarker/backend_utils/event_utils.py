"""
Handles backend logic for event management endpoints.
"""

import datetime
import traceback
from typing import Tuple, Dict, List, Any, Union

import pymongo
import pytz
from bson.objectid import ObjectId
from bson.errors import InvalidId
from flask import Request, current_app
from flask_jwt_extended import get_jwt_identity

from . import USER_COLLECTION, EVENT_COLLECTION, TIMEZONE
from . import db as db_utils
from . import utils as utils

# --- Date/Time Validation ---


def validate_date_time(
    date_string: str, expected_format: str = "%m/%d/%Y %H:%M:%S"
) -> Tuple[bool, List[Dict], Dict]:
    """Validates a date string expect in MM/DD/YYYY HH:MM:SS format.

    Also converts the valid date string into a datetime object and calculates
    total seconds since epoch (approximation used for simple comparisons).

    Parameters
    ----------
    date_string: str
        The date string to validate.

    Returns
    -------
    tuple: (bool, list, dict)
        Success flag, error list, and parsed date info.
    """
    error_list = []
    date_info: Dict = {}

    try:
        # Attempt to parse the string using the expected format
        parsed_dt = datetime.datetime.strptime(date_string, expected_format)

        # Simple validation for Reasonable year range
        if parsed_dt.year < 2000 or parsed_dt.year > 2100:
            error_list.append({"error_code": f"invalid-year-value: {date_string}"})
            return False, error_list, date_info

        # Calculate seconds approximation
        yy, mm, dd = parsed_dt.year, parsed_dt.month, parsed_dt.day
        hr, mn, sc = parsed_dt.hour, parsed_dt.minute, parsed_dt.second
        seconds = (
            yy * 365 * 24 * 3600  # Approximation: Ignores leap years
            + mm * 30 * 24 * 3600  # Approximation: Assumes 30 days/month
            + dd * 24 * 3600
            + hr * 3600
            + mn * 60
            + sc
        )

        # Use timezone-aware datetime based on configuration
        tz = pytz.timezone(TIMEZONE)
        aware_dt = tz.localize(parsed_dt)

        date_info = {
            "datetime": aware_dt,  # Store timezone-aware datetime
            "seconds": seconds,  # Store the custom seconds calculation
        }
        return True, [], date_info

    except ValueError:
        error_list.append(
            {
                "error_code": f"invalid-datetime-format: expected '{expected_format}', got '{date_string}'"
            }
        )
        return False, error_list, date_info

    except Exception as e:
        error_list.append({"error_code": f"date-parsing-error: {str(e)}"})
        # Log unexpected errors for debugging
        custom_app = db_utils.cast_app(current_app)
        custom_app.api_logger.error(
            f"Unexpected date validation error for '{date_string}': {e}\n{traceback.format_exc()}"
        )
        return False, error_list, date_info


# --- Event CRUD Functions ---


def event_addnew(api_request: Request) -> Tuple[Dict[str, Any], int]:
    """Handles adding a new event, requires JWT authentication and write access.

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "event_addnew")
    if status_code != 200:
        return request_data, status_code

    try:
        current_user_email = get_jwt_identity()
        if not current_user_email:
            # Under correct usage, this shouldn't happen based on @jwt_required
            return (
                db_utils.log_error(
                    error_log="JWT identity missing",
                    error_msg="authentication-error",
                    origin="event_addnew",
                ),
                401,
            )

        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        # Check write access
        user_info, user_info_http_code = db_utils.find_one(
            query_object={"email": current_user_email},
            projection_object={"_id": 0},
            collection=USER_COLLECTION,
        )
        if user_info_http_code != 200:
            return user_info, user_info_http_code

        if not user_info or user_info.get("access") != "write":
            error_obj = db_utils.log_error(
                error_log=f"User {current_user_email} attempted to add event without write access",
                error_msg="no-write-access",
                origin="event_addnew",
            )
            return error_obj, 403

        # Validate date fields
        validated_dates = {}
        for field in ["start_date", "end_date"]:
            success, errors, date_info = validate_date_time(request_data[field])
            if not success:
                return {"error_list": errors}, 400
            validated_dates[field] = date_info

        # Check if end date is before start date
        if (
            validated_dates["end_date"]["datetime"]
            <= validated_dates["start_date"]["datetime"]
        ):
            return {
                "error_list": [
                    {
                        "error_code": "invalid-date-range: end_date cannot be before or same as start_date"
                    }
                ]
            }, 400

        # Prepare document for insertion
        event_doc = request_data.copy()
        event_doc["start_date"] = validated_dates["start_date"]["datetime"]
        event_doc["start_date_s"] = validated_dates["start_date"]["seconds"]
        event_doc["end_date"] = validated_dates["end_date"]["datetime"]
        event_doc["end_date_s"] = validated_dates["end_date"]["seconds"]

        # Add audit timestamps (timezone-aware)
        now_ts = datetime.datetime.now(pytz.timezone(TIMEZONE))
        event_doc["createdts"] = now_ts
        event_doc["updatedts"] = now_ts
        event_doc["created_by"] = current_user_email

        # Insert into database
        result = dbh[EVENT_COLLECTION].insert_one(event_doc)

        return {
            "type": "success",
            "message": "Event added successfully",
            "id": str(result.inserted_id),
        }, 201

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Unexpected error adding event.\nData: {request_data}\nError: {e}\n{traceback.format_exc()}",
            error_msg="event-add-error",
            origin="event_addnew",
        )
        return error_obj, 500


def event_detail(api_request: Request) -> Tuple[Dict[str, Any], int]:
    """Handles retrieving details for a specific event by is ID.

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "event_detail")
    if status_code != 200:
        return request_data, status_code

    try:
        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db
        event_id_str = request_data["id"]

        try:
            event_oid = ObjectId(event_id_str)
        except InvalidId:
            error_obj = db_utils.log_error(
                error_log=f"Invalid ObjectID format for event ID: `{event_id_str}`",
                error_msg="invalid-id-format",
                origin="event_detail",
            )
            return error_obj, 400

        event = dbh[EVENT_COLLECTION].find_one(
            {"_id": event_oid}, {"_id": 0, "start_date_s": 0, "end_date_s": 0}
        )

        if not event:
            error_obj = db_utils.log_error(
                error_log=f"Event not found with ID: `{event_id_str}`",
                error_msg="record-not-found",
                origin="event_detail",
            )
            return error_obj, 404

        # Format datetime fields for display
        # Use ISO 8601 format
        datetime_format = "%Y-%m-%dT%H:%M:%S%z"
        for field in ["createdts", "updatedts", "start_date", "end_date"]:
            if field in event and isinstance(event[field], datetime.datetime):
                if (
                    event[field].tzinfo is None
                    or event[field].tzinfo.utcoffset(event[field]) is None
                ):
                    tz = pytz.timezone(TIMEZONE)
                    event[field] = tz.localize(event[field])
                event[field] = event[field].strftime(datetime_format)

        event["id"] = event_id_str

        return event, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error retrieving event details for ID `{request_data.get('id')}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="event-detail-error",
            origin="event_detail",
        )
        return (
            db_utils._create_error_obj(
                error_obj["error"]["error_id"], "internal-server-error"
            ),
            500,
        )


def event_list(api_request: Request) -> Tuple[Union[Dict, List], int]:
    """Handles listing events based on visbility and status filters.

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "event_list")
    if status_code != 200:
        return request_data, status_code

    try:
        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        # Calculate current time in seconds
        now_dt = datetime.datetime.now(pytz.timezone(TIMEZONE))
        yy, mm, dd = now_dt.year, now_dt.month, now_dt.day
        hr, mn, sc = now_dt.hour, now_dt.minute, now_dt.second
        now_in_seconds = (
            yy * 365 * 24 * 3600
            + mm * 30 * 24 * 3600  # Approximation
            + dd * 24 * 3600
            + hr * 3600
            + mn * 60
            + sc
        )

        # Build query conditions
        cond_list = []
        visibility = request_data.get("visibility", "all").lower()
        status = request_data.get("status", "all").lower()

        if visibility != "all":
            cond_list.append({"visibility": visibility})

        if status == "current":
            cond_list.append({"start_date_s": {"$lte": now_in_seconds}})
            cond_list.append({"end_date_s": {"$gte": now_in_seconds}})

        # Combine conditions if any, otherwise emtpy query
        query = {"$and": cond_list} if cond_list else {}

        event_cursor = (
            dbh[EVENT_COLLECTION]
            .find(query, {"_id": 1, "start_date_s": 1, "end_date_s": 1})
            .sort("createdts", pymongo.DESCENDING)
        )

        # Process results
        result_list = []
        datetime_format = "%Y-%m-%dT%H:%M:%S%z"
        for event in event_cursor:
            event["id"] = str(event["_id"])
            event.pop("_id")

            # Format datetime fields
            for field in ["createdts", "updatedts", "start_date", "end_date"]:
                if field in event and isinstance(event[field], datetime.datetime):
                    if (
                        event[field].tzinfo is None
                        or event[field].tzinfo.utcoffset(event[field]) is None
                    ):
                        tz = pytz.timezone(TIMEZONE)
                        event[field] = tz.localize(event[field])
                    event[field] = event[field].strftime(datetime_format)

            event.pop("start_date_s", None)
            event.pop("end_date_s", None)
            result_list.append(event)

        return result_list, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error listing events.\nFilters: {request_data}\nError: {e}\n{traceback.format_exc()}",
            error_msg="event-list-error",
            origin="event_list",
        )
        return error_obj, 500


def event_update(api_request: Request) -> Tuple[Dict[str, Any], int]:
    """Handles updating an event's visibility. Requires JWT authentication and write access.

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "event_update")
    if status_code != 200:
        return request_data, status_code

    try:
        current_user_email = get_jwt_identity()
        if not current_user_email:
            return (
                db_utils.log_error(
                    error_log="JWT identity missing",
                    error_msg="authentication-error",
                    origin="event_update",
                ),
                401,
            )

        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        # Check write access
        user_info = dbh[USER_COLLECTION].find_one({"email": current_user_email})
        if not user_info or user_info.get("access") != "write":
            error_obj = db_utils.log_error(
                error_log=f"User {current_user_email} attempted to update event without write access",
                error_msg="no-write-access",
                origin="event_update",
            )
            return error_obj, 403

        event_id_str = request_data["id"]
        new_visibility = request_data["visibility"]

        try:
            event_oid = ObjectId(event_id_str)
        except InvalidId:
            error_obj = db_utils.log_error(
                error_log=f"Invalid ObjectID format for event ID: `{event_id_str}`",
                error_msg="invalid-id-format",
                origin="event_update",
            )
            return error_obj, 400

        # Prepare update data
        update_data = {
            "visibility": new_visibility,
            "updatedts": datetime.datetime.now(pytz.timezone(TIMEZONE)),
            "updated_by": current_user_email,
        }

        # Perform the update operation
        result = dbh[EVENT_COLLECTION].update_one(
            {"_id": event_oid}, {"$set": update_data}
        )

        if result.matched_count == 0:
            error_obj = db_utils.log_error(
                error_log=f"Event not found for update with ID: `{event_id_str}`",
                error_msg="record-not-found",
                origin="event_update",
            )
            return error_obj, 404

        return {"type": "success", "message": "Event updated successfully"}, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error updating event `{request_data.get('id')}`.\nData: {request_data}\nError: {e}\n{traceback.format_exc()}",
            error_msg="event-update-error",
            origin="event_update",
        )
        return error_obj, 500


def event_delete(api_request: Request) -> Tuple[Dict[str, Any], int]:
    """Handle event deletion (soft delete by setting visibility to hidden).

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "event_delete")
    if status_code != 200:
        return request_data, status_code

    try:
        current_user_email = get_jwt_identity()
        if not current_user_email:
            return (
                db_utils.log_error(
                    error_log="JWT identity missing",
                    error_msg="authentication-error",
                    origin="event_delete",
                ),
                401,
            )

        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        # Check write access
        user_info = dbh[USER_COLLECTION].find_one({"email": current_user_email})
        if not user_info or user_info.get("access") != "write":
            error_obj = db_utils.log_error(
                error_log=f"User {current_user_email} attempted to delete event without write access",
                error_msg="no-write-access",
                origin="event_delete",
            )
            return error_obj, 403

        # Check if event exists
        event_id_str = request_data["id"]

        try:
            event_oid = ObjectId(event_id_str)
        except InvalidId:
            error_obj = db_utils.log_error(
                error_log=f"Invalid ObjectID format for event ID: `{event_id_str}`",
                error_msg="invalid-id-format",
                origin="event_delete",
            )
            return error_obj, 400

        # Soft delete by setting visibility to hidden
        update_data = {
            "visibility": "hidden",
            "updatedts": datetime.datetime.now(pytz.timezone(TIMEZONE)),
            "updated_by": current_user_email,
        }

        result = dbh[EVENT_COLLECTION].update_one(
            {"_id": event_oid}, {"$set": update_data}
        )

        if result.matched_count == 0:
            error_obj = db_utils.log_error(
                error_log=f"Event not found for delete with ID: `{event_id_str}`",
                error_msg="record-not-found",
                origin="event_delete",
            )
            return error_obj, 404

        return {"type": "sucess", "message": "Event deleted successfully"}, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error deleting event `{request_data.get('id')}`.\nError: {e}\n{traceback.format_exc()}",
            error_msg="event-delete-error",
            origin="event_delete",
        )
        return error_obj, 500
