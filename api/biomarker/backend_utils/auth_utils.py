""" Handles the backend logic for the biomarker auth endpoints.
"""

from flask import Request, current_app
from typing import Tuple, Dict
from dotenv import load_dotenv
import os
import traceback
import smtplib
from email.mime.text import MIMEText

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

    load_dotenv()
    source_app_password = os.environ.get("EMAIL_APP_PASSWORD")
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
        return response_json, 200
    except Exception as e:
        error_obj = db_utils.log_error(
            error_log=f"Failure to send contact email. {e}\n{traceback.format_exc()}",
            error_msg="internal-email-error",
            origin="contact",
        )
        return error_obj, 500
