"""Handles the backend logic for the biomarker auth endpoints."""

from flask import Request, current_app
from github import Github
from github import Auth
from typing import Tuple, Dict
import os
import traceback
import smtplib
import base64
import time
import bcrypt
import hashlib
import datetime
import pytz
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    get_csrf_token,
)
import random
import string
from email.mime.text import MIMEText

from . import (
    ADMIN_LIST,
    USER_COLLECTION,
    EMAIL_API_KEY,
    ADMIN_API_KEY,
    GITHUB_ISSUES_TOKEN,
)
from . import utils as utils
from . import db as db_utils
from . import CONTACT_SOURCE, CONTACT_RECIPIENTS


def contact(api_request: Request) -> Tuple[Dict, int]:
    """Entry point for the backend logic of the contact endpoint.

    Parameters
    ----------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "contact"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    response_txt = f"\n\n{request_arguments['fname']},\n"
    response_txt += "We have received your message and will make every effort to respond to you within a reasonable amount of time."
    response_json = {"type": "alert-success", "message": response_txt}
    response_code = 200

    source_app_password = EMAIL_API_KEY
    if source_app_password is None:
        error_obj = db_utils.log_error(
            error_log="Error reading email password environment variable.",
            error_msg="internal-email-error",
            origin="contact",
        )
        return error_obj, 500

    custom_app = db_utils.cast_app(current_app)
    custom_app.api_logger.info(
        "********************************** Contact Log **********************************"
    )
    custom_app.logger.info(request_arguments)

    detailed_message = f"From {request_arguments['fname']} {request_arguments['lname']}"
    detailed_message += f"\nEmail: {request_arguments['email']}, Subject: {request_arguments['subject']}"
    page = request_arguments.get("page", None)
    if page is not None:
        detailed_message += f"\nPage: {page}"
    detailed_message += f"\nMessage: {request_arguments['message']}"

    msg = MIMEText(detailed_message)
    msg["Subject"] = request_arguments["subject"]
    msg["To"] = ", ".join(CONTACT_RECIPIENTS)
    msg["From"] = f"{CONTACT_SOURCE}@gmail.com"

    try:
        smtp_server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        smtp_server.login(user=CONTACT_SOURCE, password=source_app_password)
        smtp_server.sendmail(msg["From"], CONTACT_RECIPIENTS, msg.as_string())
        smtp_server.quit()
    except Exception as e:
        response_json = db_utils.log_error(
            error_log=f"Failure to send contact email. {e}\n{traceback.format_exc()}",
            error_msg="internal-email-error",
            origin="contact",
        )
        response_code = 500

    # TODO : validate github token
    if GITHUB_ISSUES_TOKEN is None:
        _ = db_utils.log_error()
        return response_json, response_code
    else:
        auth = Auth.Token(GITHUB_ISSUES_TOKEN)
        # Create the Github instance
        g = Github(auth=auth)

    # TODO : try to create github ticket
    try:
        repo = g.get_repo("clinical-biomarkers/biomarker-issue-repo")
        repo.create_issue(
            title=f"{request_arguments['subject']}",
            body=f"{request_arguments['message']}",
            labels=["User Feedback"],
            assignee="jeet-vora"
        )
    except Exception as e:
        _ = db_utils.log_error()

    return response_json, response_code


def contact_notification(api_request: Request) -> Tuple[Dict, int]:
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "notification"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    api_key = request_arguments["api_key"]
    if ADMIN_API_KEY is None:
        error_object = db_utils.log_error(
            error_log="Unable to find ADMIN_API_KEY in environment variables",
            error_msg="internal-server-error",
            origin="contact_notification",
        )
        return error_object, 500

    if ADMIN_API_KEY != api_key:
        error_object = db_utils.log_error(
            error_log="Provided API key does not match ADMIN_API_KEY",
            error_msg="unathorized",
            origin="contact_notification",
        )
        return error_object, 401

    source_app_password = os.environ.get("EMAIL_APP_PASSWORD")
    if source_app_password is None:
        error_obj = db_utils.log_error(
            error_log="Error reading email password environment variable.",
            error_msg="internal-email-error",
            origin="contact",
        )
        return error_obj, 500

    emails = request_arguments["email"]

    msg = MIMEText(request_arguments["message"])
    msg["Subject"] = request_arguments["subject"]
    msg["To"] = ", ".join(emails)
    msg["From"] = f"{CONTACT_SOURCE}@gmail.com"
    response_json = {"type": "notification-success", "message": "Message sent"}

    try:
        smtp_server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        smtp_server.login(user=CONTACT_SOURCE, password=source_app_password)
        smtp_server.sendmail(msg["From"], emails, msg.as_string())
        smtp_server.quit()
        return response_json, 200
    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Failure to send notification email. {e}\n{traceback.format_exc()}",
            error_msg="internal-email-error",
            origin="contact_notification",
        )
        return error_obj, 500


def _get_random_string(length: int = 32) -> str:
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def make_hash_string() -> str:
    """Create a unique hash string for token generation."""
    m = hashlib.md5()
    s = str(time.time())
    m.update(s.encode("utf-8"))
    s = str(os.urandom(64))
    m.update(s.encode("utf-8"))

    s1 = base64.b64encode(m.digest())[:-3]
    s = s1.decode("utf-8").replace("/", "$").replace("+", "$")
    return s


def register(api_request: Request) -> Tuple[Dict, int]:
    """Handle user registration.

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "auth_register")
    if status_code != 200:
        return request_data, status_code

    try:
        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        email = request_data["email"].lower()

        # Set default user status
        user_status = 0  # 0: inactive, 1: active
        user_access = "readonly"
        user_role = ""

        if ADMIN_LIST is not None:
            if email in ADMIN_LIST:
                user_status, user_access, user_role = 1, "write", "admin"
        else:
            custom_app.api_logger.warning(
                "ADMIN LIST is None, all users will be read only"
            )

        # Check if user already exists
        existing_user_error_obj, existing_user_http_code = db_utils.find_one(
            query_object={"email": email},
            projection_object={},
            collection=USER_COLLECTION,
        )
        # Found a conflicting user
        if existing_user_http_code == 200:
            error_obj = db_utils.log_error(
                error_log=f"User attempted to register with existing email: {email}",
                error_msg="email-already-registered",
                origin="register",
            )
            return error_obj, 409  # Conflict status code
        elif existing_user_http_code == 500:
            return existing_user_error_obj, 500

        # Hash password
        hashed_password = bcrypt.hashpw(
            request_data["password"].encode("utf-8"), bcrypt.gensalt()
        )

        # Create user document
        user_doc = {
            "email": email,
            "password": hashed_password,
            "status": user_status,
            "access": user_access,
            "role": user_role,
            "created_at": datetime.datetime.now(pytz.timezone("US/Eastern")),
            "updated_at": datetime.datetime.now(pytz.timezone("US/Eastern")),
        }

        # Insert user into database
        dbh[USER_COLLECTION].insert_one(user_doc)

        return {"type": "success"}, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error during user registration: {str(e)}",
            error_msg="registration-error",
            origin="register",
        )
        return error_obj, 500


def login(api_request: Request) -> Tuple[Dict, int]:
    """Handle user login.

    Parameters
    ----------
    api_request : Request
        The flask request object.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "auth_login")
    if status_code != 200:
        return request_data, status_code

    try:
        email = request_data["email"].lower()
        password = request_data["password"]

        # Find user
        user_doc, user_http_code = db_utils.find_one(
            query_object={"email": email},
            projection_object={},
            collection=USER_COLLECTION,
        )
        if user_http_code == 404:
            error_obj = db_utils.log_error(
                error_log=f"Login attempt with unknown email: {email}",
                error_msg="incorrect-email/password",
                origin="login",
            )
            return error_obj, 401  # Unauthorized
        elif user_http_code == 500:
            return user_doc, user_http_code

        if "password" not in user_doc:
            error_obj = db_utils.log_error(
                error_log=f"User record for {email} is missing password field",
                error_msg="password does not exist for registered user",
                origin="login",
            )
            return error_obj, 500

        submitted_password = password.encode("utf-8")
        stored_password = user_doc["password"]

        # Handle string/bytes type for stored_password
        if isinstance(stored_password, str):
            stored_password = stored_password.encode("utf-8")

        if bcrypt.hashpw(submitted_password, stored_password) != stored_password:
            error_obj = db_utils.log_error(
                error_log=f"Incorrect password for user: {email}",
                error_msg="incorrect-email/password",
                origin="login",
            )
            return error_obj, 401

        # Check if account is active
        if user_doc["status"] == 0:
            error_obj = db_utils.log_error(
                error_log=f"Login attempt with inactive account: {email}",
                error_msg="inactive account",
                origin="login",
            )
            return error_obj, 403

        access_token = create_access_token(identity=email)
        refresh_token = create_refresh_token(identity=email)

        return {
            "type": "success",
            "access_token": access_token,
            "access_csrf": get_csrf_token(access_token),
            "refresh_csrf": get_csrf_token(refresh_token),
            "username": email,
        }, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error during login: {str(e)}",
            error_msg="login-error",
            origin="login",
        )
        return error_obj, 500


def userid() -> Tuple[Dict, int]:
    """Generate a unique user ID.

    Returns
    -------
    tuple : (dict, int)
        The return JSON and HTTP code.
    """
    try:
        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        max_attempts = 100

        for _ in range(max_attempts):
            user_id = _get_random_string(32).lower()
            # Check if ID already exists
            if dbh[USER_COLLECTION].count_documents({"userid": user_id}) == 0:
                # ID is unique, store it
                timestamp = datetime.datetime.now(pytz.timezone("US/Eastern")).strftime(
                    "%Y-%m-%d %H:%M:%S %Z%z"
                )
                user_obj = {"userid": user_id, "created_ts": timestamp}
                dbh[USER_COLLECTION].insert_one(user_obj)
                return {"user": user_id}, 200

        # If we reach here, we couldn't generate a unique ID after max_attempts
        error_obj = db_utils.log_error(
            error_log=f"Failed to generate unique user ID after {max_attempts} attempts",
            error_msg="userid-generator-failed",
            origin="userid",
        )
        return error_obj, 500

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Error generating user ID: {str(e)}",
            error_msg="userid-generator-error",
            origin="userid",
        )
        return error_obj, 500
