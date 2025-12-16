from typing import Any
from unittest.mock import mock_open
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
from xml.etree.ElementTree import tostring

import gcsfs
import pytest
from pytest import MonkeyPatch

import altinn.utils as utils
from altinn.flatten import create_isee_filename


def generate_sample_xml(ra_nummer: str = "RS345") -> str:
    root = Element("Root")
    intern_info = SubElement(root, "InternInfo")
    ra_nummer_el = SubElement(intern_info, "raNummer")
    ra_nummer_el.text = ra_nummer
    return tostring(root).decode()


@pytest.fixture
def mock_file_client(monkeypatch: MonkeyPatch) -> None:
    class MockFileSystem:
        def open(self, file_path: str, mode: str = "r") -> Any:
            xml_content = generate_sample_xml()
            return mock_open(read_data=xml_content)()

    monkeypatch.setattr(utils, "is_gcs", lambda x: True)
    monkeypatch.setattr(gcsfs, "GCSFileSystem", MockFileSystem)


@pytest.mark.usefixtures("mock_file_client")
def test_create_isee_filename_gcs() -> None:
    file_path = "gs://path/to/form_12345.xml"
    expected_filename = "RA345A3_12345.csv"
    assert create_isee_filename(file_path) == expected_filename


def test_create_isee_filename_local(monkeypatch: MonkeyPatch) -> None:
    file_path = "/path/to/form_12345.xml"
    monkeypatch.setattr(utils, "is_gcs", lambda x: False)
    xml_content = generate_sample_xml()
    monkeypatch.setattr("builtins.open", mock_open(read_data=xml_content))
    expected_filename = "RA345A3_12345.csv"
    assert create_isee_filename(file_path) == expected_filename
