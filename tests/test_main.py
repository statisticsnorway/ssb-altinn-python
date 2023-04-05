import os

import pytest
from pytest import MonkeyPatch

from altinn.__main__ import XmlFile
from altinn.__main__ import is_dapla


class TestIsDapla:
    @pytest.fixture(autouse=True)
    def setup_method(self, monkeypatch: MonkeyPatch) -> None:
        monkeypatch.delenv("JUPYTER_IMAGE_SPEC", raising=False)

    def test_is_dapla_true(self) -> None:
        os.environ["JUPYTER_IMAGE_SPEC"] = "dapla-jupyterlab:latest"
        assert is_dapla()

    def test_is_dapla_false(self) -> None:
        os.environ["JUPYTER_IMAGE_SPEC"] = "some-other-jupyterlab:latest"
        assert not is_dapla()

    def test_is_dapla_no_env_variable(self) -> None:
        assert not is_dapla()


class TestXmlFile:
    def test_filename(self) -> None:
        xml_file = XmlFile("file.xml")
        assert xml_file.filename() == "file"

    def test_filename_nested(self) -> None:
        xml_file = XmlFile("path/to/file.xml")
        assert xml_file.filename() == "file"
