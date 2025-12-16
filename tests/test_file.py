"""This module contains the tests for the FileInfo functions."""

from unittest.mock import MagicMock
from unittest.mock import mock_open

import gcsfs
from _pytest.monkeypatch import MonkeyPatch

from altinn.file import FileInfo


class TestFileInfo:
    """A test class for the FileInfo class."""

    # other test methods...
    def test_pretty_print_local(self, monkeypatch: MonkeyPatch) -> None:
        """Test pretty_print method for local files in XmlFile class."""
        xml_string = """<?xml version="1.0" encoding="UTF-8"?>
        <root>
            <child>Hello, world!</child>
        </root>
        """
        # Mock open for local file handling
        mock_file = mock_open(read_data=xml_string)
        monkeypatch.setattr("builtins.open", mock_file)

        # Mock os.path.expanduser to return a mock path
        monkeypatch.setattr("os.path.expanduser", lambda x: x)

        # Create an instance of FileInfo for a local file and call pretty_print on it
        local_file_info = FileInfo("path/to/local_file.xml")
        local_file_info.pretty_print()

    def test_pretty_print_gcs(self, monkeypatch: MonkeyPatch) -> None:
        """Test pretty_print method for GCS files in XmlFile class."""
        xml_string = """<?xml version="1.0" encoding="UTF-8"?>
        <root>
            <child>Hello, world!</child>
        </root>
        """

        # Mock the cat_file method for GCS file handling
        def mock_cat_file(*args, **kwargs) -> str:  # type: ignore[no-untyped-def]
            return xml_string

        # Continue with your existing GCS mocking
        file_client_mock = MagicMock()
        file_client_mock.cat_file.side_effect = mock_cat_file
        get_gcs_file_system_mock = MagicMock(return_value=file_client_mock)
        monkeypatch.setattr(gcsfs, "GCSFileSystem", get_gcs_file_system_mock)

        # Create an instance of FileInfo for a GCS file and call pretty_print on it
        gcs_file_info = FileInfo("gs://path/to/gcs_file.xml")
        gcs_file_info.pretty_print()
