import sys
import os
from utils.general import load_json_type_safe
from utils import ROOT_DIR


def get_config() -> dict:
    """Loads the config file."""
    config_obj = load_json_type_safe(
        filepath=os.path.join(ROOT_DIR, "api", "config.json"), return_type="dict"
    )
    return config_obj


def get_server_list() -> list[str]:
    """Returns a list of the servers."""
    config = get_config()
    return list(config["api_port"].keys())
