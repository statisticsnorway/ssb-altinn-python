"""This module contains the tests for the file function."""

from unittest.mock import MagicMock

from dapla import FileClient

from altinn.file import FileInfo


class TestFileInfo:
    """A test class for the FileInfo class."""

    def test_filename(self) -> None:
        """Test function.

        Checks if the filename method of XmlFile class returns
        the correct file name without the extension.
        """
        xml_file = FileInfo("file.xml")
        assert xml_file.filename() == "file"

    def test_filename_nested(self) -> None:
        """Test function.

        Checks if the filename method of XmlFile class returns
        the correct file name without the extension, when the
        file is nested in directories.
        """
        xml_file = FileInfo("path/to/file.xml")
        assert xml_file.filename() == "file"

    def test_pretty_print(self, monkeypatch):
        """Test pretty_print method of XmlFile class."""
        xml_string = """<?xml version="1.0" encoding="UTF-8"?>
        <root>
            <child>Hello, world!</child>
        </root>
        """

        # Mock the cat_file method to return the xml as bytes
        def mock_cat_file(*args, **kwargs):
            return xml_string.encode()

        # Patch the FileClient.get_gcs_file_system method to return a mock
        # object that has the cat_file method patched
        file_client_mock = MagicMock()
        file_client_mock.cat_file.side_effect = mock_cat_file
        get_gcs_file_system_mock = MagicMock(return_value=file_client_mock)
        monkeypatch.setattr(FileClient, "get_gcs_file_system", get_gcs_file_system_mock)

        # Create an instance of FileInfo and call pretty_print on it
        file_info = FileInfo("path/to/file.xml")
        file_info.pretty_print()
