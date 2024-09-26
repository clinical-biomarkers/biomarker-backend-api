import argparse
from argparse import ArgumentParser
import os
from utils.config import get_server_list


def standard_parser() -> tuple[ArgumentParser, list[str]]:
    """Creates a standard parser that just takes `server` as a required positional argument."""
    server_list = get_server_list()
    parser = argparse.ArgumentParser(prog=os.path.basename(__file__))
    parser.add_argument("server", help="/".join(server_list))
    return parser, server_list
