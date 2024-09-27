import argparse
from argparse import ArgumentParser
import sys
import os
from typing import NoReturn, Optional
from tutils.config import get_server_list


def standard_parser() -> tuple[ArgumentParser, list[str]]:
    """Creates a standard parser that just takes `server` as a required positional argument."""
    server_list = get_server_list()
    parser = argparse.ArgumentParser(prog=os.path.basename(__file__))
    parser.add_argument("server", help="/".join(server_list))
    return parser, server_list


def parse_server(
    parser: ArgumentParser, server: Optional[str], server_list: list[str]
) -> NoReturn | str:
    """Parses the standard server argument."""
    if server is None:
        parser.print_help()
        sys.exit(1)
    server = server.strip().lower()
    if server not in server_list:
        print("Invalid server.")
        parser.print_help()
        sys.exit(1)
    return server
