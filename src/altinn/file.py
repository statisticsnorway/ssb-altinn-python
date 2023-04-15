"""This module contains the main function for running the Altinn application."""

import os
from typing import Optional

from dapla import FileClient
from defusedxml.minidom import parseString


def main() -> None:
    """Placeholder function for the main function.

    This function is called when the altinn package is run as a script.
    """
    pass


def is_dapla() -> bool:
    """Check whether the current environment is running a Dapla JupyterLab instance.

    Returns:
        bool: True if the current environment is running a Dapla JupyterLab instance,
        False otherwise.
    """
    jupyter_image_spec: Optional[str] = os.environ.get("JUPYTER_IMAGE_SPEC")
    return bool(jupyter_image_spec and "dapla-jupyterlab" in jupyter_image_spec)


class FileInfo:
    """This class represents an Altinn application."""

    def __init__(self, file_path: str) -> None:
        """Initialize an XmlFile object with the given file path.

        Args:
            file_path (str): The path to the XML file.
        """
        if is_dapla():
            self.file_path = file_path
        else:
            print(
                """FileInfo class can only be instantiated in a Dapla JupyterLab
                  environment."""
            )
            # Alternatively, you can print a message and return,
            # without raising an exception
            # print("XmlFile class can only be instantiated in a Dapla
            # JupyterLab environment.")
            # return

    def filename(self) -> str:
        """Get the name of the XML file.

        Returns:
            str: The name of the XML file.
        """
        split_path = self.file_path.split("/")
        return split_path[-1][:-4]

    def pretty_print(self) -> None:
        """Print formatted version of an xml-file."""
        fs = FileClient.get_gcs_file_system()
        dom = parseString(fs.cat_file(self.file_path))
        pretty_xml = dom.toprettyxml(indent="  ")
        print(pretty_xml)
