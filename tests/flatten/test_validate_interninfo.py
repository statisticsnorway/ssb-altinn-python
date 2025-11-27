from unittest.mock import patch

import pytest

# Import the function under test
from altinn.flatten import _validate_interninfo


@pytest.fixture
def mock_xml_ra() -> dict[str, dict[str, dict[str, str]]]:
    return {
        "Root": {
            "InternInfo": {"enhetsIdent": "123", "enhetsType": "ABC", "delregNr": "001"}
        }
    }


@pytest.fixture
def mock_xml_rs() -> dict[str, dict[str, dict[str, str]]]:
    return {
        "Root": {
            "InternInfo": {
                "enhetsOrgnr": "987654321",
                "enhetsType": "XYZ",
                "delregNr": "002",
            }
        }
    }


def test_validate_interninfo_ra_valid(
    mock_xml_ra: dict[str, dict[str, dict[str, str]]],
) -> None:
    with (
        patch("altinn.flatten._read_single_xml_to_dict", return_value=mock_xml_ra),
        patch("altinn.flatten._check_altinn_type", return_value="RA"),
    ):
        assert _validate_interninfo("dummy.xml") is True


def test_validate_interninfo_rs_valid(
    mock_xml_rs: dict[str, dict[str, dict[str, str]]],
) -> None:
    with (
        patch("altinn.flatten._read_single_xml_to_dict", return_value=mock_xml_rs),
        patch("altinn.flatten._check_altinn_type", return_value="RS"),
    ):
        assert _validate_interninfo("dummy.xml") is True


def test_validate_interninfo_ra_missing_key(
    mock_xml_ra: dict[str, dict[str, dict[str, str]]],
) -> None:
    # Remove one required key
    incomplete_xml = mock_xml_ra.copy()
    incomplete_xml["Root"] = incomplete_xml["Root"].copy()
    incomplete_xml["Root"]["InternInfo"] = {"enhetsIdent": "123"}  # Missing 2 keys

    with (
        patch("altinn.flatten._read_single_xml_to_dict", return_value=incomplete_xml),
        patch("altinn.flatten._check_altinn_type", return_value="RA"),
    ):
        assert _validate_interninfo("dummy.xml") is False


def test_validate_interninfo_rs_missing_key(
    mock_xml_rs: dict[str, dict[str, dict[str, str]]],
) -> None:
    incomplete_xml = mock_xml_rs.copy()
    incomplete_xml["Root"] = incomplete_xml["Root"].copy()
    incomplete_xml["Root"]["InternInfo"] = {"enhetsOrgnr": "987"}  # Missing 2 keys

    with (
        patch("altinn.flatten._read_single_xml_to_dict", return_value=incomplete_xml),
        patch("altinn.flatten._check_altinn_type", return_value="RS"),
    ):
        assert _validate_interninfo("dummy.xml") is False


def test_invalid_altinn_type() -> None:
    with (
        patch("altinn.flatten._read_single_xml_to_dict", return_value={"Root": {}}),
        patch("altinn.flatten._check_altinn_type", return_value="XX"),
    ):
        with pytest.raises(ValueError):
            _validate_interninfo("dummy.xml")
