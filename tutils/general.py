import json
import sys
import os
import decimal
import subprocess
from typing import Union, Literal, overload, Optional, NoReturn


def load_json(filepath: str) -> Union[dict, list]:
    """Loads a JSON file.

    Parameters
    ----------
    filepath: str
        The path to the JSON file.

    Returns
    -------
    dict or list
        The JSON object.
    """
    with open(filepath, "r") as f:
        json_obj = json.load(f)
    return json_obj


@overload
def load_json_type_safe(filepath: str, return_type: Literal["dict"]) -> dict:
    pass


@overload
def load_json_type_safe(filepath: str, return_type: Literal["list"]) -> list:
    pass


def load_json_type_safe(
    filepath: str, return_type: Literal["dict", "list"]
) -> Union[dict, list]:
    """Handles the type checking for the expected return types.

    Parameters
    ----------
    filepath: str
        The filepath to the JSON file to laod.
    return_type: Literal["dict", "list"]
        The expected return type.
    """
    loaded_json = load_json(filepath)
    if return_type == "dict" and not isinstance(loaded_json, dict):
        raise ValueError(
            f"Expected type `dict` for file {filepath}, got type `{type(loaded_json)}`."
        )
    elif return_type == "list" and not isinstance(loaded_json, list):
        raise ValueError(
            f"Expected type `list` for file {filepath}, got type `{type(loaded_json)}`."
        )
    return loaded_json


def _json_serialize_default(item):
    if isinstance(item, decimal.Decimal):
        return float(item)
    raise TypeError(
        f"Object of type {item.__class__.__name__} or not JSON serializable"
    )


def write_json(
    filepath: str, data: Union[list, dict], include_default: bool = False
) -> None:
    """Writes a JSON file.

    Parameters
    ----------
    filepath: str
        The path to the JSON file.
    data: dict or list
        The data to write to the JSON file.
    include_default: bool, optional
        Whether to include the default fallback for non-serializable objects.
    """
    with open(filepath, "w") as f:
        if include_default:
            json.dump(data, f, indent=4, default=_json_serialize_default)
        else:
            json.dump(data, f, indent=4)


def get_user_confirmation() -> None | NoReturn:
    """Prompts the user for a confirmation or denial."""
    while True:
        user_input = input("Continue? (y/n) ").strip().lower()
        if user_input == "y":
            return None
        elif user_input == "n":
            sys.exit(0)
        else:
            print("Please enter 'y' for yes or 'n' for no.")


def resolve_symlink(path: str) -> Optional[str]:
    """Takes a symlink path (i.e. the `current/` symlink) and returns the version that
    it points to.

    Parameters
    ----------
    path: str
        The symlink path to check.

    Returns
    -------
    str
        The directory the symlink points to.
    """
    if not os.path.islink(path):
        return None
    read_link = os.readlink(path)
    return read_link


def copy_file(src: str, dest: str) -> None:
    """Copies a file from src to dest."""
    subprocess.run(["cp", src, dest], check=True)


def confirmation_message_complete() -> None:
    print(
        "Confirmation prompts over, can safely send to background execution if needed."
    )
