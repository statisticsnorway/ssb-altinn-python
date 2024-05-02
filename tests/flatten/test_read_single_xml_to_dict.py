from unittest.mock import mock_open
from unittest.mock import patch

import pytest

from altinn.flatten import _read_single_xml_to_dict


@pytest.fixture
def mock_xml_data() -> str:
    return "<root><child>value</child></root>"


# Test reading from a local file
def test_read_single_xml_to_dict_local(mock_xml_data: str) -> None:
    # Mocking open() and xmltodict.parse
    with patch("builtins.open", mock_open(read_data=mock_xml_data)) as mocked_file:
        with patch("xmltodict.parse") as mocked_parse:
            mocked_parse.return_value = {"root": {"child": "value"}}
            result = _read_single_xml_to_dict("path/to/local/file.xml")
            mocked_file.assert_called_once_with("path/to/local/file.xml")
            assert result == {"root": {"child": "value"}}
            mocked_parse.assert_called_once_with(mock_xml_data)


# Test reading from a GCS location
def test_read_single_xml_to_dict_gcs(mock_xml_data: str) -> None:
    # Mocking GCS interactions
    with patch("altinn.flatten.FileClient.get_gcs_file_system") as mocked_gcs_client:
        mocked_fs = mocked_gcs_client.return_value
        mocked_fs.open = mock_open(read_data=mock_xml_data)
        with patch("altinn.flatten.utils.is_gcs") as mocked_is_gcs:
            with patch("xmltodict.parse") as mocked_parse:
                mocked_is_gcs.return_value = True
                mocked_parse.return_value = {"root": {"child": "value"}}
                result = _read_single_xml_to_dict("gs://bucket/file.xml")
                mocked_gcs_client.assert_called_once()
                assert result == {"root": {"child": "value"}}
                mocked_parse.assert_called_once_with(mock_xml_data)
