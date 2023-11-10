"""Utilities for working with Altinn-data in Python."""

import os
from typing import Optional

from dapla import FileClient
from defusedxml.ElementTree import ParseError
from defusedxml.minidom import parseString


def is_dapla() -> bool:
    """Check whether the current environment is running a Dapla JupyterLab instance.

    Returns:
        bool: True if the current environment is running a Dapla JupyterLab instance,
        False otherwise.
    """
    jupyter_image_spec: Optional[str] = os.environ.get("JUPYTER_IMAGE_SPEC")
    return bool(jupyter_image_spec and "jupyterlab-dapla" in jupyter_image_spec)


def is_valid_xml(file_path) -> bool:
    """Check whether the file is valid XML.

    Args:
        file_path (str): The path to the XML file.

    Returns:
        bool: True if the XML is valid,
        False otherwise.
    """
    if is_dapla():
        fs = FileClient.get_gcs_file_system()
        try:
            parseString(fs.cat_file(file_path))
            return True
        except ParseError:
            return False
    else:
        try:
            with open(file_path) as file:
                parseString(file.read())
                return True
        except ParseError:
            return False
