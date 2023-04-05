"""This module contains the main function for running the Altinn application."""

import os
from typing import Optional


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


class XmlFile:
    """This class represents an Altinn application."""

    def __init__(self, file_path: str) -> None:
        """Initialize an XmlFile object with the given file path.

        Args:
            file_path (str): The path to the XML file.
        """
        self.file_path = file_path

    def filename(self) -> str:
        """Get the name of the XML file.

        Returns:
            str: The name of the XML file.
        """
        split_path = self.file_path.split("/")
        return split_path[-1][:-4]
