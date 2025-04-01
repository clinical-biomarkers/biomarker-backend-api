from collections.abc import Sequence
from flask import Request, current_app
from flask_jwt_extended import get_jwt_identity
from typing import Tuple, Dict, List, Any, Union
import datetime
import pytz
import traceback
from bson.objectid import ObjectId
import pymongo

from . import USER_COLLECTION, EVENT_COLLECTION
from . import db as db_utils
from . import utils as utils


def validate_date_time(date_string: str) -> Tuple[bool, List[Dict], Dict]:
    """Validate a date string in format MM/DD/YYYY HH:MM:SS

    Parameters
    ----------
    date_string : str
        Date string in format MM/DD/YYYY HH:MM:SS

    Returns
    -------
    tuple : (bool, list, dict)
        Success flag, error list, and parsed date info
    """
    error_list = []
    date_info = {}  # type: ignore

    try:
        date_parts: Sequence[Union[int, str]] = (
            date_string.strip().split(" ")[0].strip().split("/")
        )
        time_parts: Sequence[Union[int, str]] = (
            date_string.strip().split(" ")[1].strip().split(":")
        )

        if len(date_parts) != 3:
            error_list.append({"error_code": f"invalid-date-format: {date_string}"})
            return False, error_list, date_info

        if len(time_parts) != 3:
            error_list.append({"error_code": f"invalid-time-format: {date_string}"})
            return False, error_list, date_info

        # Validate numeric values
        for j in range(0, 3):
            if not date_parts[j].isdigit():  # type: ignore
                error_list.append({"error_code": f"invalid-date-format: {date_string}"})
                return False, error_list, date_info

            if not time_parts[j].isdigit():  # type: ignore
                error_list.append({"error_code": f"invalid-time-format: {date_string}"})
                return False, error_list, date_info

            date_parts[j] = int(date_parts[j])  # type: ignore
            time_parts[j] = int(time_parts[j])  # type: ignore

        # Validate ranges
        if date_parts[0] < 1 or date_parts[0] > 12:  # type: ignore
            error_list.append({"error_code": f"invalid-month-value: {date_string}"})
        if date_parts[1] < 1 or date_parts[1] > 31:  # type: ignore
            error_list.append({"error_code": f"invalid-day-value: {date_string}"})
        if date_parts[2] < 2021:  # type: ignore
            error_list.append({"error_code": f"invalid-year-value: {date_string}"})
        if time_parts[0] < 0 or time_parts[0] > 23:  # type: ignore
            error_list.append({"error_code": f"invalid-hour-value: {date_string}"})
        if time_parts[1] < 0 or time_parts[1] > 59:  # type: ignore
            error_list.append({"error_code": f"invalid-minute-value: {date_string}"})
        if time_parts[2] < 0 or time_parts[2] > 59:  # type: ignore
            error_list.append({"error_code": f"invalid-second-value: {date_string}"})

        if error_list:
            return False, error_list, date_info

        # Convert to datetime and seconds
        dt, tm = date_string.split(" ")[0], date_string.split(" ")[1]
        mm, dd, yy = dt.split("/")
        hr, mn, sc = tm.split(":")
        seconds = (
            int(yy) * 365 * 24 * 3600
            + int(mm) * 31 * 24 * 3600
            + int(dd) * 24 * 3600
            + int(hr) * 3600
            + int(mn) * 60
            + int(sc)
        )

        date_info = {
            "datetime": datetime.datetime.strptime(date_string, "%m/%d/%Y %H:%M:%S"),
            "seconds": seconds,
        }

        return True, [], date_info

    except Exception as e:
        error_list.append({"error_code": f"date-parsing-error: {str(e)}"})
        return False, error_list, date_info


def event_addnew(api_request: Request) -> Tuple[Dict[str, Any], int]:
    """Handle adding a new event.

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "event_addnew")
    if status_code != 200:
        return request_data, status_code

    try:

        current_user = get_jwt_identity()

        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        # Check write access
        user_info = dbh[USER_COLLECTION].find_one({"email": current_user})
        if not user_info or "access" not in user_info or user_info["access"] != "write":
            error_obj = db_utils.log_error(
                error_log=f"User {current_user} attempted to add event without write access",
                error_msg="no-write-access",
                origin="event_addnew",
            )
            return error_obj, 403

        # Validate date fields
        date_fields = ["start_date", "end_date"]
        for field in date_fields:
            success, errors, date_info = validate_date_time(request_data[field])
            if not success:
                return {"error_list": errors}, 400

            # Add parsed date info to request data
            request_data[f"{field}_s"] = date_info["seconds"]
            request_data[field] = date_info["datetime"]

        # Add timestamps
        request_data["createdts"] = datetime.datetime.now(pytz.timezone("US/Eastern"))
        request_data["updatedts"] = request_data["createdts"]

        # Insert into database
        dbh[EVENT_COLLECTION].insert_one(request_data)

        return {"type": "success", "message": "Event added successfully"}, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error adding event: {str(e)}\n{traceback.format_exc()}",
            error_msg="event-add-error",
            origin="event_addnew",
        )
        return error_obj, 500


def event_detail(api_request: Request) -> Tuple[Dict[str, Any], int]:
    """Handle retrieving event details.

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "event_detail")
    if status_code != 200:
        return request_data, status_code

    try:
        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        event_id = request_data["id"]
        try:
            event = dbh[EVENT_COLLECTION].find_one({"_id": ObjectId(event_id)})
        except Exception as e:
            error_obj = db_utils.log_error(
                error_log=f"Invalid event ID format: {event_id}",
                error_msg="invalid-id-format",
                origin="event_detail",
            )
            return error_obj, 400

        if not event:
            error_obj = db_utils.log_error(
                error_log=f"Event not found with ID: {event_id}",
                error_msg="record-not-found",
                origin="event_detail",
            )
            return error_obj, 404

        # Format the event data
        event["id"] = str(event["_id"])
        del event["_id"]

        # Format datetime fields
        for field in ["createdts", "updatedts", "start_date", "end_date"]:
            if field in event and hasattr(event[field], "strftime"):
                event[field] = event[field].strftime("%Y-%m-%d %H:%M:%S %Z%z")

        return event, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error retrieving event details: {str(e)}\n{traceback.format_exc()}",
            error_msg="event-detail-error",
            origin="event_detail",
        )
        return {"error_list": [{"error_code": str(e)}]}, 500

def event_list(api_request: Request) -> Tuple[Union[Dict, List], int]:
    """Handle listing events.

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    # Parse and validate request
    request_data, status_code = utils.get_request_object(api_request, "event_list")
    if status_code != 200:
        return request_data, status_code

    try:
        # Get database connection
        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        # Get current time in seconds
        now_est = datetime.datetime.now(pytz.timezone("US/Eastern")).strftime(
            "%m/%d/%Y %H:%M:%S"
        )
        dt, tm = now_est.split(" ")[0], now_est.split(" ")[1]
        mm, dd, yy = dt.split("/")
        hr, mn, sc = tm.split(":")
        seconds = (
            int(yy) * 365 * 24 * 3600
            + int(mm) * 31 * 24 * 3600
            + int(dd) * 24 * 3600
            + int(hr) * 3600
            + int(mn) * 60
            + int(sc)
        )

        # Build query conditions
        cond_list = []
        if request_data["visibility"] != "all":
            cond_list.append({"visibility": {"$eq": request_data["visibility"]}})

        if "status" in request_data and request_data["status"] == "current":
            cond_list.append({"start_date_s": {"$lte": seconds}})
            cond_list.append({"end_date_s": {"$gte": seconds}})

        query = {} if not cond_list else {"$and": cond_list}

        # Execute query
        events = list(
            dbh[EVENT_COLLECTION].find(query).sort("createdts", pymongo.DESCENDING)
        )

        # Format results
        result_list = []
        for event in events:
            if "title" not in event or "start_date_s" not in event:
                continue

            event["id"] = str(event["_id"])
            del event["_id"]

            # Format datetime fields
            for field in ["createdts", "updatedts", "start_date", "end_date"]:
                if field in event and hasattr(event[field], "strftime"):
                    event[field] = event[field].strftime("%Y-%m-%d %H:%M:%S %Z%z")

            event["now_ts"] = seconds
            result_list.append(event)

        return result_list, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error listing events: {str(e)}\n{traceback.format_exc()}",
            error_msg="event-list-error",
            origin="event_list",
        )
        return error_obj, 500


def event_update(api_request: Request) -> Tuple[Dict[str, Any], int]:
    """Handle updating an event.

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    # Parse and validate request
    request_data, status_code = utils.get_request_object(api_request, "event_update")
    if status_code != 200:
        return request_data, status_code

    try:
        current_user = get_jwt_identity()

        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        # Check write access
        user_info = dbh[USER_COLLECTION].find_one({"email": current_user})
        if not user_info or "access" not in user_info or user_info["access"] != "write":
            error_obj = db_utils.log_error(
                error_log=f"User {current_user} attempted to update event without write access",
                error_msg="no-write-access",
                origin="event_update",
            )
            return error_obj, 403

        # Prepare update data
        event_id = request_data["id"]
        update_data = {k: v for k, v in request_data.items() if k != "id"}
        update_data["updatedts"] = datetime.datetime.now(pytz.timezone("US/Eastern"))

        # Update event
        try:
            result = dbh[EVENT_COLLECTION].update_one(
                {"_id": ObjectId(event_id)}, {"$set": update_data}
            )

            if result.matched_count == 0:
                error_obj = db_utils.log_error(
                    error_log=f"Event not found with ID: {event_id}",
                    error_msg="record-not-found",
                    origin="event_update",
                )
                return error_obj, 404

            return {"type": "success", "message": "Event updated successfully"}, 200

        except Exception as e:
            error_obj = db_utils.log_error(
                error_log=f"Invalid event ID format: {event_id}",
                error_msg="invalid-id-format",
                origin="event_update",
            )
            return error_obj, 400

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error updating event: {str(e)}\n{traceback.format_exc()}",
            error_msg="event-update-error",
            origin="event_update",
        )
        return error_obj, 500


def event_delete(api_request: Request) -> Tuple[Dict[str, Any], int]:
    """Handle event deletion (soft delete by setting visibility to hidden).

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "event_delete")
    if status_code != 200:
        return request_data, status_code

    try:
        current_user = get_jwt_identity()

        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        # Check write access
        user_info = dbh[USER_COLLECTION].find_one({"email": current_user})
        if not user_info or "access" not in user_info or user_info["access"] != "write":
            error_obj = db_utils.log_error(
                error_log=f"User {current_user} attempted to delete event without write access",
                error_msg="no-write-access",
                origin="event_delete",
            )
            return error_obj, 403

        # Check if event exists
        event_id = request_data["id"]
        try:
            event = dbh[EVENT_COLLECTION].find_one({"_id": ObjectId(event_id)})
        except Exception as e:
            error_obj = db_utils.log_error(
                error_log=f"Invalid event ID format: {event_id}",
                error_msg="invalid-id-format",
                origin="event_delete",
            )
            return error_obj, 400

        if not event:
            error_obj = db_utils.log_error(
                error_log=f"Event not found with ID: {event_id}",
                error_msg="record-not-found",
                origin="event_delete",
            )
            return error_obj, 404

        # Soft delete by setting visibility to hidden
        update_data = {
            "visibility": "hidden",
            "updatedts": datetime.datetime.now(pytz.timezone("US/Eastern")),
        }

        dbh[EVENT_COLLECTION].update_one(
            {"_id": ObjectId(event_id)}, {"$set": update_data}
        )

        return {"type": "sucess", "message": "Event deleted successfully"}, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error deleting event: {str(e)}\n{traceback.format_exc()}",
            error_msg="event-delete-error",
            origin="event_delete",
        )
        return error_obj, 500
