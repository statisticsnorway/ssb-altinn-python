"""Utilities for working with Altinn-data in Python."""

import os

from dapla import FileClient
from defusedxml.ElementTree import ParseError
from defusedxml.minidom import parseString


def is_gcs(file_path: str) -> bool:
    """Check whether the given file path is a Google Cloud Storage path.

    Args:
        file_path (str): The file path to check.

    Returns:
        bool: True if the file path is a Google Cloud Storage path, False otherwise.
    """
    return file_path.startswith("gs://")


def is_valid_xml(file_path: str) -> bool:
    """Check whether the file is valid XML.

    Args:
        file_path (str): The path to the XML file.

    Returns:
        bool: True if the XML is valid, False otherwise.
    """
    if is_gcs(file_path):
        fs = FileClient.get_gcs_file_system()
        try:
            # Read and parse the file from Google Cloud Storage
            parseString(fs.cat_file(file_path))
            return True
        except ParseError:
            return False
    else:
        try:
            # Expand the path to support '~' for home directory
            expanded_path = os.path.expanduser(file_path)
            with open(expanded_path) as file:
                # Read and parse the local file
                parseString(file.read())
                return True
        except (ParseError, OSError):
            return False
