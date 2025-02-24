from logging import Logger
import subprocess
import os
from typing import Optional

from tutils import ROOT_DIR
from tutils.logging import log_msg

NOTIFY_PATH = os.path.join(ROOT_DIR, "misc_scripts", "notify.py")


def send_notification(
    email: list[str],
    subject: str,
    message: str,
    server: str,
    logger: Optional[Logger] = None,
) -> None:
    command = ["python", NOTIFY_PATH, server]

    for recipient in email:
        command.extend(["--email", recipient])

    command.extend(["--subject", subject, "--message", message])

    if logger:
        cmd_str = " ".join(command)
        log_msg(logger=logger, msg=f"Executing notification command: {cmd_str}")

    try:
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode == 0:
            msg = "Notification sent successfully"
            if logger:
                log_msg(logger=logger, msg=msg)
            else:
                print(msg)
        else:
            msg = f"Notification failed with exit code {result.returncode}: {result.stderr}"
            if logger:
                log_msg(logger=logger, msg=msg, level="error")
            else:
                print(msg)

    except Exception as e:
        err_msg = f"Unexpected error when sending notification:\n{e}"
        if logger:
            log_msg(logger=logger, msg=err_msg, level="error")
        else:
            print(err_msg)
