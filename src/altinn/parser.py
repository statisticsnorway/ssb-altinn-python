"""This module contains the main function for running the Altinn application."""

import os
from typing import Any
from typing import Optional
from xml.etree.ElementTree import Element

import pandas as pd
from dapla import FileClient
from defusedxml import ElementTree

from .utils import is_gcs
from .utils import is_valid_xml


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
        expanded_path = os.path.expanduser(file_path)
        self.file_path = expanded_path
        if not is_valid_xml(self.file_path):
            print("""File is not a valid XML-file.""")

    def traverse_xml(
        self,
        element: Element,
        column_counter: int = 1,
        data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Recursively traverse an XML element and extract data.

        Args:
            element: The XML element to traverse.
            column_counter (int): The counter for generating unique column names.
            data (dict or None): The dictionary to store the extracted data.

        Returns:
            dict: The dictionary containing the extracted data.
        """
        if data is None:
            data = {}

        def recursive_traverse(
            element: Element,
            column_counter: int,
            data: dict[str, Any],
            prefix: str,
        ) -> None:
            for sub_element in element:
                tag_name = sub_element.tag
                full_tag_name = prefix + "_" + tag_name if prefix else tag_name

                if len(sub_element) > 0:
                    recursive_traverse(sub_element, column_counter, data, full_tag_name)
                else:
                    if full_tag_name in data:
                        if isinstance(data[full_tag_name], list):
                            data[full_tag_name].append(sub_element.text)
                        else:
                            data[full_tag_name] = [
                                data[full_tag_name],
                                sub_element.text,
                            ]
                    else:
                        data[full_tag_name] = sub_element.text

                    if full_tag_name in data and isinstance(data[full_tag_name], list):
                        for i, value in enumerate(data[full_tag_name], start=1):
                            new_column_name = f"{full_tag_name}_{i}"
                            data[new_column_name] = value
                        del data[full_tag_name]  # delete original non-numbered key
                        column_counter += 1

        for child in element:
            recursive_traverse(child, column_counter, data, child.tag)
        return data

    def get_root_from_dapla(self) -> Element:
        """Read in XML-file from GCP-buckets on Dapla.

        Returns:
            Element: The root Element of the parsed XML file.
        """
        fs = FileClient.get_gcs_file_system()
        with fs.open(self.file_path, mode="r") as f:
            single_xml = f.read()
        return ElementTree.fromstring(single_xml)  # type: ignore[no-any-return]

    def get_root_from_filesystem(self) -> Element:
        """Read in XML-file from classical filesystem.

        Returns:
            Element: The root Element of the parsed XML file.
        """
        tree = ElementTree.parse(self.file_path)
        return tree.getroot()  # type: ignore[no-any-return]

    def to_dataframe(self) -> pd.DataFrame:
        """Parse single XML file to a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame representation of the XML file.
        """
        if is_gcs(self.file_path):
            root = self.get_root_from_dapla()
        else:
            root = self.get_root_from_filesystem()
        data: dict[str, Any] = {}
        self.traverse_xml(root, 1, data)
        df = pd.DataFrame([data])
        return df
