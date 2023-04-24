"""Parsing of Altinn xml-files."""

import pandas as pd
from dapla import FileClient
from defusedxml import ElementTree

from .utils import is_dapla


def main() -> None:
    """Placeholder function for the main function.

    This function is called when the altinn package is run as a script.
    """
    pass


class ParseSingleXml:
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

    def to_dict(self) -> dict:
        """Parse single XML file to a dictionary.

        Returns:
            dict: A dictionary representation of the XML file.
        """
        fs = FileClient.get_gcs_file_system()
        with fs.open(self.file_path, mode="r") as f:
            single_xml = f.read()

        root = ElementTree.fromstring(single_xml)
        intern_info = root.find("InternInfo")
        kontakt = root.find("Kontakt")
        skjemadata = root.find("Skjemadata")

        data = []
        all_tags = set()

        for element in intern_info:
            all_tags.add(element.tag)

        for element in kontakt:
            all_tags.add(element.tag)

        for element in skjemadata:
            all_tags.add(element.tag)

        result_dict = {}

        for tag in all_tags:
            element = intern_info.find(tag)
            if element is None:
                element = kontakt.find(tag)
            if element is None:
                element = skjemadata.find(tag)
            if element is not None:
                value = element.text
                data.append(value)
                result_dict[tag] = value
            else:
                data.append(None)
                result_dict[tag] = None

        return result_dict

    def to_dataframe(self) -> pd.DataFrame:
        """Parse single XML file to a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame representation of the XML file.
        """
        xml_dict = self.to_dict()
        df = pd.DataFrame([xml_dict])
        return df
