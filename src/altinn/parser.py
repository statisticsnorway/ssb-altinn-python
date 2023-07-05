"""Parsing of Altinn xml-files."""

import pandas as pd
from dapla import FileClient
from defusedxml import ElementTree

from .utils import is_dapla
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
        self.file_path = file_path
        if not is_valid_xml():
            print("""File is not a valid XML-file.""")

    # Function to recursively traverse the XML tree and capture all tags
    def traverse_xml(element, column_counter=1, data={}) -> dict[str, str]:
        # Iterate over the sub-elements of the current element
        for sub_element in element:
            # Create the tag name
            tag_name = sub_element.tag

            # Check if the sub-element has children
            if len(sub_element) > 0:
                # Recursively traverse the child elements
                traverse_xml(sub_element, column_counter, data)
            else:
                # Store the text content in the dictionary
                if tag_name in data:
                    if isinstance(data[tag_name], list):
                        # Append the value to the existing list
                        data[tag_name].append(sub_element.text)
                    else:
                        # Convert the existing value to a list and append the new value
                        data[tag_name] = [data[tag_name], sub_element.text]
                else:
                    # If the tag doesn't exist, store the value in the dictionary
                    data[tag_name] = sub_element.text

                # Check if the tag name has duplicates
                if tag_name in data and isinstance(data[tag_name], list):
                    # Generate a unique column name for each occurrence
                    column_name = f"{tag_name}_{column_counter}"

                    # Create a new column for each value in the list
                    for i, value in enumerate(data[tag_name], start=1):
                        new_column_name = f"{tag_name}_{i}"
                        data[new_column_name] = value

                    # Remove the original duplicate tag column
                    data.pop(tag_name)

                    # Increment the column counter
                    column_counter += 1
        return data

    def get_root_from_dapla(self):
        """Read in XML-file from GCP-buckets on Dapla.

        Returns:
            ElementTree: A ElementTree-object representation of the XML file.
        """
        fs = FileClient.get_gcs_file_system()
        with fs.open(self.file_path, mode="r") as f:
            single_xml = f.read()
        root = ElementTree.fromstring(single_xml)
        return root

    def get_root_from_filsystem(self):
        """Read in XML-file from classical filesystem.

        Returns:
            ElementTree: A ElementTree-object representation of the XML file.
        """
        tree = ElementTree.parse(self.file_path)
        root = tree.getroot()
        return root

    if is_dapla():
        root = get_root_from_dapla(self.file_path)
    else:
        root = get_root_from_filesystem(self.file_path)

    def to_dataframe(self) -> pd.DataFrame:
        """Parse single XML file to a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame representation of the XML file.
        """
        data = {}
        traverse_xml(root, 1, data)
        df = pd.DataFrame([data])
        return df
