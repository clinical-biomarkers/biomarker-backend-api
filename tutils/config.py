import os
import sys
from tutils.general import load_json_type_safe
from logging import Logger
from typing import Optional, NoReturn
from tutils import ROOT_DIR
from tutils.logging import log_msg


def get_config(logger: Optional[Logger] = None) -> dict | NoReturn:
    """Loads the config file."""
    try:
        config_obj = load_json_type_safe(
            filepath=os.path.join(ROOT_DIR, "api", "config.json"), return_type="dict"
        )
        return config_obj
    except Exception as e:
        msg = f"Failed to get config with error: {e}"
        if logger:
            log_msg(logger=logger, msg=msg, level="error")
        print(msg)
        sys.exit(1)


def get_server_list() -> list[str]:
    """Returns a list of the servers."""
    config = get_config()
    return list(config["api_port"].keys())
