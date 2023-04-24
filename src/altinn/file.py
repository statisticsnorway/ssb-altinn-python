"""This module contains the main function for running the Altinn application."""

from dapla import FileClient
from defusedxml.ElementTree import ParseError
from defusedxml.minidom import parseString

from .utils import is_dapla


def main() -> None:
    """Placeholder function for the main function.

    This function is called when the altinn package is run as a script.
    """
    pass


class FileInfo:
    """This class represents an Altinn application."""

    def __init__(self, file_path: str) -> None:
        """Initialize an XmlFile object with the given file path.

        Args:
            file_path (str): The path to the XML file.
        """
        self.file_path = file_path
        if not is_dapla():
            print(
                """FileInfo class can only be instantiated in a Dapla JupyterLab
                  environment."""
            )

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

    def print(self) -> None:
        """Print unformatted version of an XML file."""
        fs = FileClient.get_gcs_file_system()
        file = fs.cat_file(self.file_path)
        print(file.decode())

    def validate(self) -> bool:
        """Validate the XML file."""
        fs = FileClient.get_gcs_file_system()
        try:
            parseString(fs.cat_file(self.file_path))
            return True

        except ParseError:
            return False
