"""Sends a notification, should only be run from the server."""

import requests
import json
from argparse import ArgumentParser
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils.logging import setup_logging, log_msg

LOGGER = setup_logging("notification.log")
ENDPOINT = "http://localhost:8089/auth/contact_notification"


def main():
    parser = ArgumentParser()
    parser.add_argument("--email", nargs="+", required=True)
    parser.add_argument("--subject", type=str, required=True)
    parser.add_argument("--message", type=str, required=True)

    options = parser.parse_args()
    payload = {
        "email": options.email,
        "subject": options.subject,
        "message": options.message,
    }

    try:
        response = requests.post(ENDPOINT, json=payload)
        if response.status_code != 200:
            msg = (
                f"Non-200 status code: {response.status_code}\n"
                f"Response body: {json.dumps(response.json(), indent=2)}\n"
                "Args:\n"
                f"\tEmails: {options.email}\n\tSubject: {options.subject}\n\tMessage: {options.message}"
            )
            log_msg(logger=LOGGER, msg=msg, level="error")
    except Exception as e:
        log_msg(
            logger=LOGGER,
            msg=f"Error in sending notification email: {e}",
            level="error",
        )


if __name__ == "__main__":
    main()
