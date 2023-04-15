"""This module contains the tests for the main function."""

import os
from unittest.mock import MagicMock

import pytest
from dapla import FileClient
from pytest import MonkeyPatch

from altinn.file import FileInfo
from altinn.file import is_dapla


class TestIsDapla:
    """A test class for the is_dapla() function."""

    @pytest.fixture(autouse=True)
    def setup_method(self, monkeypatch: MonkeyPatch) -> None:
        """A fixture that runs before every test method.

        It deletes the JUPYTER_IMAGE_SPEC environment variable if it exists.
        """
        monkeypatch.delenv("JUPYTER_IMAGE_SPEC", raising=False)

    def test_is_dapla_true(self) -> None:
        """Test function to check is_dapla().

        Returns True when the JUPYTER_IMAGE_SPEC
        environment variable contains 'dapla-jupyterlab:latest'.
        """
        os.environ["JUPYTER_IMAGE_SPEC"] = "dapla-jupyterlab:latest"
        assert is_dapla()

    def test_is_dapla_false(self) -> None:
        """Test function to check is_dapla().

        Returns False when the JUPYTER_IMAGE_SPEC
        environment variable does not contain 'dapla-jupyterlab:latest'.
        """
        os.environ["JUPYTER_IMAGE_SPEC"] = "some-other-jupyterlab:latest"
        assert not is_dapla()

    def test_is_dapla_no_env_variable(self) -> None:
        """Test function to check is_dapla().

        Returns False when the JUPYTER_IMAGE_SPEC
        environment variable is not set.
        """
        assert not is_dapla()


class TestXmlFile:
    """A test class for the XmlFile class."""

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
