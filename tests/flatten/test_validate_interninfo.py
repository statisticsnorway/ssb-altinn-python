# from typing import Any
# from unittest.mock import patch

# import pytest

# from altinn.flatten import _validate_interninfo


# @pytest.fixture
# def xml_data() -> dict[str, Any]:
#     # Basic structure of the XML dictionary that includes the 'InternInfo' key
#     return {
#         "RootElement": {
#             "InternInfo": {
#                 "enhetsIdent": "12345",
#                 "enhetsType": "TypeA",
#                 "delregNr": "67890",
#             }
#         }
#     }


# @pytest.mark.parametrize(
#     "data, expected",
#     [
#         # Case: All required keys are present
#         (
#             {
#                 "RootElement": {
#                     "InternInfo": {
#                         "enhetsIdent": "12345",
#                         "enhetsType": "TypeA",
#                         "delregNr": "67890",
#                     }
#                 }
#             },
#             True,
#         ),
#         # Case: One key missing
#         (
#             {
#                 "RootElement": {
#                     "InternInfo": {"enhetsIdent": "12345", "enhetsType": "TypeA"}
#                 }
#             },
#             False,
#         ),
#         # Case: Multiple keys missing
#         ({"RootElement": {"InternInfo": {"enhetsType": "TypeA"}}}, False),
#         # Case: Completely missing 'InternInfo'
#         ({"RootElement": {}}, False),
#         # Case: Nested incorrectly
#         (
#             {
#                 "RootElement": {
#                     "InternInfo": {
#                         "OtherKey": {
#                             "enhetsIdent": "12345",
#                             "enhetsType": "TypeA",
#                             "delregNr": "67890",
#                         }
#                     }
#                 }
#             },
#             False,
#         ),
#     ],
# )
# def test_validate_interninfo(
#     data: dict[str, Any], expected: bool, xml_data: dict[str, Any]
# ) -> None:
#     with patch("altinn.flatten._read_single_xml_to_dict", return_value=data):
#         assert _validate_interninfo("fake_path") == expected

import pytest
from unittest.mock import patch

# Import the function under test
from altinn.flatten import _validate_interninfo


@pytest.fixture
def mock_xml_ra():
    return {
        "Root": {
            "InternInfo": {
                "enhetsIdent": "123",
                "enhetsType": "ABC",
                "delregNr": "001"
            }
        }
    }


@pytest.fixture
def mock_xml_rs():
    return {
        "Root": {
            "InternInfo": {
                "enhetsOrgnr": "987654321",
                "enhetsType": "XYZ",
                "delregNr": "002"
            }
        }
    }


def test_validate_interninfo_ra_valid(mock_xml_ra):
    with patch("altinn.flatten._read_single_xml_to_dict", return_value=mock_xml_ra), \
         patch("altinn.flatten._check_altinn_type", return_value="RA"):
        assert _validate_interninfo("dummy.xml") is True


def test_validate_interninfo_rs_valid(mock_xml_rs):
    with patch("altinn.flatten._read_single_xml_to_dict", return_value=mock_xml_rs), \
         patch("altinn.flatten._check_altinn_type", return_value="RS"):
        assert _validate_interninfo("dummy.xml") is True


def test_validate_interninfo_ra_missing_key(mock_xml_ra):
    # Remove one required key
    incomplete_xml = mock_xml_ra.copy()
    incomplete_xml["Root"] = incomplete_xml["Root"].copy()
    incomplete_xml["Root"]["InternInfo"] = {"enhetsIdent": "123"}  # Missing 2 keys

    with patch("altinn.flatten._read_single_xml_to_dict", return_value=incomplete_xml), \
         patch("altinn.flatten._check_altinn_type", return_value="RA"):
        assert _validate_interninfo("dummy.xml") is False


def test_validate_interninfo_rs_missing_key(mock_xml_rs):
    incomplete_xml = mock_xml_rs.copy()
    incomplete_xml["Root"] = incomplete_xml["Root"].copy()
    incomplete_xml["Root"]["InternInfo"] = {"enhetsOrgnr": "987"}  # Missing 2 keys

    with patch("altinn.flatten._read_single_xml_to_dict", return_value=incomplete_xml), \
         patch("altinn.flatten._check_altinn_type", return_value="RS"):
        assert _validate_interninfo("dummy.xml") is False


def test_invalid_altinn_type():
    with patch("altinn.flatten._read_single_xml_to_dict", return_value={"Root": {}}), \
         patch("altinn.flatten._check_altinn_type", return_value="XX"):
        with pytest.raises(ValueError):
            _validate_interninfo("dummy.xml")
