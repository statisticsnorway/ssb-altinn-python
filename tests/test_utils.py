"""This module contains the tests for the utils functions."""

import os

import pytest
from pytest import MonkeyPatch

from altinn.utils import is_dapla


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
        environment variable contains 'jupyterlab-dapla:latest'.
        """
        os.environ["JUPYTER_IMAGE_SPEC"] = "jupyterlab-dapla:latest"
        assert is_dapla()
