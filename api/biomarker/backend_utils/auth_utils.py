"""
Handles the backend logic for the biomarker auth endpoints.
"""

import smtplib
import traceback
import os
import base64
import time
import bcrypt
import hashlib
import datetime
import pytz
import random
import string
from typing import Optional, Tuple, Dict, List, Any
from typing_extensions import deprecated

from flask import Request, current_app
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    get_csrf_token,
)
from email.mime.text import MIMEText
from github import Github, Auth as GithubAuth

from . import (
    ADMIN_LIST,
    TIMEZONE,
    USER_COLLECTION,
    EMAIL_API_KEY,
    ADMIN_API_KEY,
    GITHUB_ISSUES_TOKEN,
    GITHUB_ISSUE_ASSIGNEE,
)
from . import utils as utils
from . import db as db_utils
from . import CONTACT_SOURCE, CONTACT_RECIPIENTS

# --- Helper Functions ---


def _send_email(subject: str, body: str, recipients: List[str]) -> Optional[str]:
    """Sends an email using configured Gmail SMTP settings.

    Parameters
    ----------
    subject: str
        The subject line of the email.
    body: str
        The plain text body of the email.
    recipients: List[str]
        A list of email addresses to send the email to.

    Returns
    -------
    Optional[str]
        None if the email was sent successfully, otherwise an error message string.
    """
    if not EMAIL_API_KEY:
        return "Email app password (EMAIL_API_KEY) is not configured."
    if not CONTACT_SOURCE:
        return "Email source user (CONTACT_SOURCE) is not configured."
    if not recipients:
        return "No recipients specified for this email."

    sender_email = f"{CONTACT_SOURCE}@gmail.com"
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["To"] = ", ".join(recipients)
    msg["From"] = sender_email

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp_server:
            smtp_server.login(user=CONTACT_SOURCE, password=EMAIL_API_KEY)
            smtp_server.sendmail(sender_email, recipients, msg.as_string())
        return None
    except smtplib.SMTPAuthenticationError:
        return f"SMTP Authentication Error: Check CONTACT_SOURCE (`{CONTACT_SOURCE}`) and EMAIL_API_KEY."
    except smtplib.SMTPException as e:
        return f"SMTP Error: Failed to send email. {e}"
    except Exception as e:
        return f"Unexpected Error: Failed to send email. {e}\n{traceback.format_exc()}"


def _create_github_issue(
    title: str, body: str, labels: List[str], assignee: Optional[str] = None
) -> Optional[str]:
    """Creates an issue in the configured Github repository.

    Parameters
    ----------
    title: str
        The title of the Github issue.
    body: str
        The body content of the Github issue.
    labels: List[str]
        A list of labels to apply to the issue.
    assignee: Optional[str], optional
        The Github username to assign the issue to (if overriding global).

    Returns
    -------
    Optional[str]
        None if the issue was created successfully, otherwise an error message string.
    """
    if not GITHUB_ISSUES_TOKEN:
        return "Github issues token (GITHUB_ISSUES_TOKEN) is not configured."
    if not GITHUB_ISSUE_ASSIGNEE:
        return "Github issue assignee(s) (GITHUB_ISSUE_ASSIGNEES) is not configured."

    issue_assignee = assignee if assignee else GITHUB_ISSUE_ASSIGNEE

    try:
        auth = GithubAuth.Token(GITHUB_ISSUES_TOKEN)
        g = Github(auth=auth)
        repo = g.get_repo("clinical-biomarkers/biomarker-issue-repo")
        repo.create_issue(
            title=title, body=body, labels=labels, assignee=issue_assignee
        )
        return None
    except Exception as e:
        return f"Failed to create Github issue: {e}\n{traceback.format_exc()}"


# --- Backend Functions ---


def contact(api_request: Request) -> Tuple[Dict, int]:
    """Handles the backend logic for the user contact form submission.

    Sends an email to configured recipients and attempts to create a Github issue.

    Parameters
    ----------
    api_request: Request
        The Flask request object containing contact form data.

    Returns
    -------
    Tuple: (dict, int)
        A response dictionary of error object and HTTP return code.
    """
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "contact"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    # Prepare email content
    fname = request_arguments.get("fname", "N/A")
    lname = request_arguments.get("lname", "N/A")
    email_addr = request_arguments.get("email", "N/A")
    subject = request_arguments.get("subject", "No Subject Provided")
    message = request_arguments.get("message", "")
    page = request_arguments.get("page")

    email_body = f"From: {fname} {lname}\n"
    email_body += f"Email: {email_addr}\n"
    email_body += f"Subject: {subject}\n"
    if page:
        email_body += f"Page: {page}\n"
    email_body += f"\nMessage:\n{message}"

    # Send email notification
    email_error = _send_email(
        subject=subject, body=email_body, recipients=CONTACT_RECIPIENTS
    )

    response_json: Dict[str, Any]
    response_code: int

    if email_error:
        response_json = db_utils.log_error(
            error_log=f"Failure sending contact email: {email_error}",
            error_msg="internal-email-error",
            origin="contact",
        )
        response_code = 500
    else:
        response_txt = f"\n\n{fname},\n"
        response_txt += "We have received your message and will make every effort to respond to you within a reasonable amount of time."
        response_json = {"type": "alert-success", "message": response_txt}
        response_code = 200

    # Attempt to create Github issue ergardless of email success or failure
    github_body = f"**Page:** {page}\n" if page else ""
    github_body += f"**Subject:** {subject}\n\n"
    github_body += f"**Message:**\n{message}"

    github_error = _create_github_issue(
        title=subject, body=github_body, labels=["User Feedback"]
    )

    if github_error:
        db_utils.log_error(
            error_log=f"Failed to create Github issue from contact form: {github_error}",
            error_msg="internal-server-error",
            origin="contact",
        )

    return response_json, response_code


def contact_notification(api_request: Request) -> Tuple[Dict, int]:
    """Handles sending arbitrary notifications via email (Admin restricted).

    Requires a valid ADMIN_API_KEY for authorization.

    Parameters
    ----------
    api_request: Request
        The Flask request object.

    Returns
    -------
    tuple: (dict, int)
        A response dictionary and HTTP status code.
    """
    request_arguments, request_http_code = utils.get_request_object(
        api_request, "notification"
    )
    if request_http_code != 200:
        return request_arguments, request_http_code

    api_key = request_arguments["api_key"]
    if ADMIN_API_KEY is None:
        error_object = db_utils.log_error(
            error_log="ADMIN_API_KEY is not configured in environment variables",
            error_msg="internal-server-error",
            origin="contact_notification",
        )
        return error_object, 500

    if ADMIN_API_KEY != api_key:
        error_object = db_utils.log_error(
            error_log="Invalid ADMIN_API_KEY provided for contact_notification.",
            error_msg="unathorized",
            origin="contact_notification",
        )
        return error_object, 401

    # --- Send Email ---
    subject = request_arguments.get("subject", "Notification")
    message = request_arguments.get("message", "")
    recipients = request_arguments.get("email", [])

    if not recipients:
        return {
            "error": {
                "error_msg": "bad-request",
                "details": "No recipient emails provided.",
            }
        }, 400

    email_error = _send_email(subject=subject, body=message, recipients=recipients)

    if email_error:
        error_obj = db_utils.log_error(
            error_log=f"Failure sending notification email: {email_error}",
            error_msg="internal-email-error",
            origin="contact_notification",
        )
        return error_obj, 500
    else:
        return {
            "type": "notification-success",
            "message": "Notification sent successfully",
        }, 200


# --- User Authentication / Management ---


def _get_random_string(length: int = 32) -> str:
    """Generates a random alphanumeric string of specified length."""
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def register(api_request: Request) -> Tuple[Dict, int]:
    """Handle user registration.

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "auth_register")
    if status_code != 200:
        return request_data, status_code

    try:
        custom_app = db_utils.cast_app(current_app)
        dbh = custom_app.mongo_db

        email = request_data["email"].lower()

        # --- Determine User Status/Access ---
        user_status = 0  # 0: inactive, 1: active (default inactive)
        user_access = "readonly"  # Default read-only
        user_role = ""  # Default no role

        if ADMIN_LIST and email in (admin.lower() for admin in ADMIN_LIST):
            user_status, user_access, user_role = 1, "write", "admin"
        else:
            custom_app.api_logger.warning(
                "ADMIN LIST is None, all users will be read only"
            )

        # --- Check for Existing User ---
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

        # --- Hash Password ---
        try:
            hashed_password = bcrypt.hashpw(
                request_data["password"].encode("utf-8"), bcrypt.gensalt()
            )
        except ValueError as e:
            error_obj = db_utils.log_error(
                error_log=f"Error hashing password for user {email}: {e}",
                error_msg="internal-server-error",
                origin="register",
            )
            return error_obj, 500

        # --- Create User Document ---
        now_ts = datetime.datetime.now(pytz.timezone(TIMEZONE))
        user_doc = {
            "email": email,
            "password": hashed_password,
            "status": user_status,
            "access": user_access,
            "role": user_role,
            "created_at": now_ts,
            "updated_at": now_ts,
        }

        # --- Insert User ---
        try:
            dbh[USER_COLLECTION].insert_one(user_doc)
        except Exception as e:
            error_obj = db_utils.log_error(
                error_log=f"Database error registering user {email}: {e}\n{traceback.format_exc()}",
                error_msg="internal-server-error",
                origin="register",
            )
            return error_obj, 500

        return {"type": "success", "message": "Registration successful."}, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Unexpected error during user registration for email {request_data.get('email', 'N/A')}: {e}\n{traceback.format_exc()}",
            error_msg="registration-error",
            origin="register",
        )
        return error_obj, 500


def login(api_request: Request) -> Tuple[Dict, int]:
    """Handle user login.

    Parameters
    ----------
    api_request: Request
        The flask request object.

    Returns
    -------
    tuple: (dict, int)
        The return JSON and HTTP code.
    """
    request_data, status_code = utils.get_request_object(api_request, "auth_login")
    if status_code != 200:
        return request_data, status_code

    try:
        email = request_data["email"].lower()
        password = request_data["password"]

        # --- Find User ---
        user_doc, user_http_code = db_utils.find_one(
            query_object={"email": email},
            projection_object={},
            collection=USER_COLLECTION,
        )

        # --- Validate User Existence and Password ---
        if user_http_code == 404:
            error_obj = db_utils.log_error(
                error_log=f"Login attempt with unknown email: {email}",
                error_msg="incorrect-email-or-password",
                origin="login",
            )
            return error_obj, 401  # Unauthorized
        elif user_http_code == 500:
            return user_doc, user_http_code

        stored_password_hash = user_doc.get("password")
        if stored_password_hash is None:
            error_obj = db_utils.log_error(
                error_log=f"User record for {email} is missing the password field.",
                error_msg="internal-server-error",
                origin="login",
            )
            return error_obj, 500

        # Ensure stored_password_hash is bytes for bcrypt.checkpw
        if isinstance(stored_password_hash, str):
            stored_password_hash = stored_password_hash.encode("utf-8")

        # Verify the password
        try:
            if not bcrypt.checkpw(password.encode("utf-8"), stored_password_hash):
                error_obj = db_utils.log_error(
                    error_log=f"Incorrect password attempt for user: {email}",
                    error_msg="incorrect-email-or-password",
                    origin="login",
                )
                return error_obj, 401
        except ValueError as e:
            error_obj = db_utils.log_error(
                error_log=f"Error comparing password hash for user {email} (hash might be invalid): {e}",
                error_msg="internal-server-error",
                origin="login",
            )
            return error_obj, 500

        # --- Check Account Status ---
        if user_doc.get("status", 0) == 0:
            error_obj = db_utils.log_error(
                error_log=f"Login attempt with inactive account: {email}",
                error_msg="account-inactive",
                origin="login",
            )
            return error_obj, 403

        # --- Generate Tokens ---
        identity = email
        access_token = create_access_token(identity=identity)
        refresh_token = create_refresh_token(identity=identity)

        # Return tokens and CSRF tokens
        return {
            "type": "success",
            "message": "Login successful",
            "access_token": access_token,
            "access_csrf": get_csrf_token(access_token),
            "refresh_token": refresh_token,
            "refresh_csrf": get_csrf_token(refresh_token),
            "username": email,
            "role": user_doc.get("role", ""),
            "access": user_doc.get("access", "readonly"),
        }, 200

    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Unexpected error during login for email {request_data.get('email', 'N/A')}: {e}\n{traceback.format_exc()}",
            error_msg="login-error",
            origin="login",
        )
        return error_obj, 500


# --- Deprecated ---


# Copied from Glygen, not used and has some potential issues:
# - Possibility of collisions
# - Race condition possibility (two requests could generate the same ID and check existence before inserts)
@deprecated("Not used")
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


@deprecated("Not used")
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
