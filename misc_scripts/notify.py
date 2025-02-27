"""Sends a notification, should only be run from the server."""

import requests
import json
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from tutils import ROOT_DIR
from tutils.logging import setup_logging, log_msg
from tutils.parser import standard_parser, parse_server
from tutils.config import get_config

LOGGER = setup_logging("notification.log")
ENDPOINT = "http://localhost:{port}/auth/contact_notification"


def main():
    parser, server_list = standard_parser()
    parser.add_argument("--email", action="append", required=True)
    parser.add_argument("--subject", type=str, required=True)
    parser.add_argument("--message", type=str, required=True)
    options = parser.parse_args()
    server = parse_server(parser=parser, server=options.server, server_list=server_list)

    dotenv_path = os.path.join(ROOT_DIR, ".env")
    load_dotenv(dotenv_path=dotenv_path)
    api_key = os.environ.get("ADMIN_API_KEY")
    if api_key is None:
        msg = f"FAILED to find admin api key, looked in {dotenv_path}"
        log_msg(logger=LOGGER, msg=msg, level="error")
        sys.exit(1)

    config_obj = get_config()
    port = config_obj["api_port"][server]

    api_endpoint = ENDPOINT.format(port=port)

    payload = {
        "email": options.email,
        "subject": options.subject,
        "message": options.message,
        "api_key": api_key,
    }

    try:
        response = requests.post(api_endpoint, json=payload)
        if response.status_code != 200:
            msg = (
                f"Non-200 status code: {response.status_code}\n"
                f"Response body: {json.dumps(response.json(), indent=2)}\n"
                "Args:\n"
                f"\tEmails: {options.email}\n\tSubject: {options.subject}\n\tMessage: {options.message}"
            )
            log_msg(logger=LOGGER, msg=msg, level="error")
            sys.exit(1)

        msg = (
            f"Notification success, status code: {response.status_code}\n"
            f"Args: `{server}`, `{options.email}`, `{options.subject}`, `{options.message}`"
        )
        log_msg(logger=LOGGER, msg=msg)
    except Exception as e:
        log_msg(
            logger=LOGGER,
            msg=f"Error in sending notification email: {e}",
            level="error",
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
