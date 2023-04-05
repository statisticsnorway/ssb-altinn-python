"""This module contains the tests for the main function."""
import os

import pytest
from pytest import MonkeyPatch

from altinn.main import XmlFile
from altinn.main import is_dapla


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
        xml_file = XmlFile("file.xml")
        assert xml_file.filename() == "file"

    def test_filename_nested(self) -> None:
        """Test function.

        Checks if the filename method of XmlFile class returns
        the correct file name without the extension, when the
        file is nested in directories.
        """
        xml_file = XmlFile("path/to/file.xml")
        assert xml_file.filename() == "file"
