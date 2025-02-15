import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from tutils.logging import setup_logging

LOGGER = setup_logging("id_assign.log")
