from typing import Any
from unittest.mock import patch

import pytest

from altinn.flatten import _validate_interninfo


@pytest.fixture
def xml_data() -> dict[str, Any]:
    # Basic structure of the XML dictionary that includes the 'InternInfo' key
    return {
        "RootElement": {
            "InternInfo": {
                "enhetsIdent": "12345",
                "enhetsType": "TypeA",
                "delregNr": "67890",
            }
        }
    }


@pytest.mark.parametrize(
    "data, expected",
    [
        # Case: All required keys are present
        (
            {
                "RootElement": {
                    "InternInfo": {
                        "enhetsIdent": "12345",
                        "enhetsType": "TypeA",
                        "delregNr": "67890",
                    }
                }
            },
            True,
        ),
        # Case: One key missing
        (
            {
                "RootElement": {
                    "InternInfo": {"enhetsIdent": "12345", "enhetsType": "TypeA"}
                }
            },
            False,
        ),
        # Case: Multiple keys missing
        ({"RootElement": {"InternInfo": {"enhetsType": "TypeA"}}}, False),
        # Case: Completely missing 'InternInfo'
        ({"RootElement": {}}, False),
        # Case: Nested incorrectly
        (
            {
                "RootElement": {
                    "InternInfo": {
                        "OtherKey": {
                            "enhetsIdent": "12345",
                            "enhetsType": "TypeA",
                            "delregNr": "67890",
                        }
                    }
                }
            },
            False,
        ),
    ],
)
def test_validate_interninfo(
    data: dict[str, Any], expected: bool, xml_data: dict[str, Any]
) -> None:
    with patch("altinn.flatten._read_single_xml_to_dict", return_value=data):
        assert _validate_interninfo("fake_path") == expected
