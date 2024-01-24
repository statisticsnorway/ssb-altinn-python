"""This module contains the main function for running the Altinn application."""

import os

from dapla import FileClient
from defusedxml.ElementTree import ParseError
from defusedxml.minidom import parseString

from .utils import is_gcs


def main() -> None:
    """Placeholder function for the main function.

    This function is called when the altinn package is run as a script.
    """
    pass


class FileInfo:
    """This class represents file information handling."""

    def __init__(self, file_path: str) -> None:
        """Initialize an XmlFile object with the given file path.

        Args:
            file_path (str): The path to the XML file.
        """
        # Store the original file path
        self.original_file_path = file_path
        # Expand the path to support '~' for home directory
        self.expanded_file_path = os.path.expanduser(file_path)

    def _read_file(self) -> str:
        """Read file content based on the file source.

        Returns:
            The content of the file as string.
        """
        if is_gcs(self.original_file_path):
            fs = FileClient.get_gcs_file_system()
            return fs.cat_file(self.original_file_path)  # type: ignore[no-any-return]
        else:
            with open(self.expanded_file_path) as f:
                return f.read()

    def filename(self) -> str:
        """Get the name of the XML file.

        Returns:
            str: The name of the XML file.
        """
        split_path = self.expanded_file_path.split("/")
        return split_path[-1][:-4]

    def pretty_print(self) -> None:
        """Print formatted version of an XML file."""
        xml_content = self._read_file()
        dom = parseString(xml_content)
        pretty_xml = dom.toprettyxml(indent="  ")
        print(pretty_xml)

    def print(self) -> None:
        """Print unformatted version of an XML file."""
        file_content = self._read_file()
        print(file_content)

    def validate(self) -> bool:
        """Validate the XML file."""
        try:
            xml_content = self._read_file()
            parseString(xml_content)
            return True

        except ParseError:
            return False
